{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Types where

import Data.Csv
import Data.List    (intercalate)
import GHC.Generics (Generic)

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
    OPEN   Order
  | CLOSED Order
  deriving Eq

instance Show Market where
  show (OPEN m)   = show m
  show (CLOSED m) = show m

data Order =
  Order {
    usd :: Integer
  , eur :: Integer
  , gbp :: Integer
  } deriving Eq

instance Show Order where
  show Order{..} = concat
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

aggregate :: Trade -> Order -> Order
aggregate (Trade _ USD n BUY)  Order{..} = Order (usd - n) eur gbp
aggregate (Trade _ USD n SELL) Order{..} = Order (usd + n) eur gbp
aggregate (Trade _ EUR n BUY)  Order{..} = Order usd (eur - n) gbp
aggregate (Trade _ EUR n SELL) Order{..} = Order usd (eur + n) gbp
aggregate (Trade _ GBP n BUY)  Order{..} = Order usd eur (gbp - n)
aggregate (Trade _ GBP n SELL) Order{..} = Order usd eur (gbp + n)

isOpen :: Market -> Bool
isOpen (OPEN _)   = True
isOpen (CLOSED _) = False

closeMarket :: Market -> Market
closeMarket (OPEN x) = CLOSED x
