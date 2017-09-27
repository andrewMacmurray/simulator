{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Control.Concurrent   hiding (yield)
import           Control.Monad        (replicateM)
import qualified Data.ByteString.Lazy as BL
import           Data.Csv
import           Data.List            (intercalate)
import           Data.Vector          (Vector)
import qualified Data.Vector          as V
import           GHC.Generics         (Generic)
import           Pipes
import           System.Environment   (getArgs)
import           System.IO

data Trade =
  Trade {
    timestamp :: Integer
  , currency  :: Currency
  , quantity  :: Integer
  , direction :: Direction
  } deriving Generic

instance Show Trade where
  show Trade{..} = intercalate ","
    [ show timestamp
    , show currency
    , show quantity
    , show direction
    ]

data Market =
    OPEN   Aggregate
  | CLOSED Aggregate
  deriving Eq

instance Show Market where
  show (OPEN m)   = show m
  show (CLOSED m) = show m

data Aggregate =
  Aggregate {
    usd :: Integer
  , eur :: Integer
  , gbp :: Integer
  } deriving Eq

instance Show Aggregate where
  show Aggregate{..} = concat
    [ "USD ", show usd, ", "
    , "EUR ", show eur, ", "
    , "GBP ", show gbp
    ]

data Direction =
    BUY
  | SELL
  deriving Show

data Currency =
    USD
  | EUR
  | GBP
  deriving Show

instance FromField Direction where
  parseField "BUY"  = return BUY
  parseField "SELL" = return SELL

instance FromField Currency where
  parseField "USD" = return USD
  parseField "EUR" = return EUR
  parseField "GBP" = return GBP

instance FromRecord Trade


main :: IO ()
main = do
  lineBuffering
  csvs <- appendDir <$> getArgs
  let newMarket = OPEN $ Aggregate 0 0 0
  marketMVars <- replicateM (length csvs) (newMVar newMarket)
  mapM_ (uncurry spawnMarket) $ zip marketMVars csvs
  printMarkets marketMVars
  where
    lineBuffering = hSetBuffering stdout LineBuffering
    appendDir     = fmap $ mappend "./markets/"


printMarkets :: [MVar Market] -> IO ()
printMarkets markets = do
  mapM_ printMarket markets
  putStrLn "---"
  delay 1000
  openMarkets <- mapM checkMarketOpen markets
  let stillTrading = or openMarkets
  if stillTrading then
    printMarkets markets
  else do
    mapM_ putStrLn
      [ ""
      , "All markets Closed"
      , "Final State: "
      ]
    mapM_ printMarket markets


printMarket :: MVar Market -> IO ()
printMarket m = readMVar m >>= print


checkMarketOpen :: MVar Market -> IO Bool
checkMarketOpen m = marketOpen <$> readMVar m


marketOpen :: Market -> Bool
marketOpen (OPEN _)   = True
marketOpen (CLOSED _) = False


aggregateTrade :: Trade -> Aggregate -> Aggregate
aggregateTrade (Trade _ USD n BUY)  Aggregate{..} = Aggregate (usd - n) eur gbp
aggregateTrade (Trade _ USD n SELL) Aggregate{..} = Aggregate (usd + n) eur gbp
aggregateTrade (Trade _ EUR n BUY)  Aggregate{..} = Aggregate usd (eur - n) gbp
aggregateTrade (Trade _ EUR n SELL) Aggregate{..} = Aggregate usd (eur + n) gbp
aggregateTrade (Trade _ GBP n BUY)  Aggregate{..} = Aggregate usd eur (gbp - n)
aggregateTrade (Trade _ GBP n SELL) Aggregate{..} = Aggregate usd eur (gbp + n)


spawnMarket :: MVar Market -> FilePath -> IO ThreadId
spawnMarket market filePath =
  forkIO . runEffect $ for (replayCsv filePath) (lift . emitTrade market)


emitTrade :: MVar Market -> Maybe Trade -> IO ()
emitTrade market mt =
  case mt of
    Just t  -> continueTrading t
    Nothing -> closeMarket
    where
      continueTrading t = atomically market (\(OPEN m) -> OPEN $ aggregateTrade t m)
      closeMarket       = atomically market (\(OPEN m) -> CLOSED m)
      atomically mv f   = takeMVar mv >>= (putMVar mv . f)


replayCsv :: FilePath -> Producer (Maybe Trade) IO ()
replayCsv path = do
  xs <- lift $ BL.readFile path
  case decode NoHeader xs of
    Left err -> lift $ putStrLn "error reading market"
    Right v  -> tickTrades 0 v


tickTrades :: Integer -> Vector Trade -> Producer (Maybe Trade) IO ()
tickTrades t v =
  if not (V.null v) then do
    let trade = V.head v
        t'    = timestamp trade
    yield $ Just trade
    lift . delay . fromIntegral $ t' - t
    tickTrades t' $ V.tail v
  else
    yield Nothing


delay :: Int -> IO ()
delay ms = threadDelay $ ms * 1000
