module Main where

import           Control.Applicative  ((<*))
import           Control.Concurrent   hiding (yield)
import           Control.Monad        (forever, void)
import           Control.Monad.State
import qualified Data.ByteString.Lazy as BL
import           Data.Csv             (HasHeader (NoHeader))
import qualified Data.Csv.Streaming   as S
import           Data.Vector          (Vector)
import           Pipes
import           System.IO
import           Types

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  aggregateAndLog $ map replayCsv paths

paths :: [String]
paths = map ("./markets/" ++)
  [ "market_a.csv"
  , "market_b.csv"
  , "market_c.csv"
  ]

aggregateAndLog :: [Producer (Maybe Order) IO ()] -> IO ()
aggregateAndLog mkts = do
  mvs <- mapM (const . newMVar . OPEN $ Order 0 0 0) mkts
  traverse forkIO $ zipWith runMarket mvs mkts
  logUntilClose mvs

logUntilClose :: [MVar Market] -> IO ()
logUntilClose mkts = do
  trading <- stillTrading mkts
  if trading then do
    delayMs 1000
    logMarkets mkts
    putStrLn "--------------"
    logUntilClose mkts
  else do
    putStrLn "Markets Closed, final state: "
    logMarkets mkts

stillTrading :: [MVar Market] -> IO Bool
stillTrading xs = any isOpen <$> mapM readMVar xs

logMarkets :: [MVar Market] -> IO ()
logMarkets = mapM_ $ readMVar >=> print

runMarket :: MVar Market -> Producer (Maybe Order) IO () -> IO ()
runMarket mv mkt = runEffect . for mkt $ update mv
  where
    update mv (Just x) = io . swapMVar mv $ OPEN x
    update mv Nothing  = io . modifyMVar_ mv $ return . closeMkt
    closeMkt (OPEN x)  = CLOSED x
    io                 = void . liftIO

replayCsv :: FilePath -> Producer (Maybe Order) IO ()
replayCsv x = decodeMarket x >>= pipe
  where
    pipe xs        = withEOF $ each xs >-> tick
    decodeMarket x = S.decode NoHeader <$> lift (BL.readFile x)

tick :: Pipe Trade Order IO ()
tick = runS $ do
  (t1, ord) <- get
  trade     <- lift await
  let t2     = timestamp trade
      newOrd = aggregate trade ord
  wait $ t2 - t1
  lift $ yield newOrd
  put (t2, newOrd)
  where
    runS = flip evalStateT (0, Order 0 0 0) . forever
    wait = liftIO . delayMs . fromIntegral

withEOF :: (Monad m) => Proxy a' a () b m r -> Proxy a' a () (Maybe b) m r
withEOF p = for p (yield . Just) <* yield Nothing

delayMs :: Int -> IO ()
delayMs = threadDelay . (*1000)
