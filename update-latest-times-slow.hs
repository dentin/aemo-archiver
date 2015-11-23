{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Main where


import           Database.Persist.Postgresql

import           AEMO.Database
import           AEMO.Types

import           Control.Lens

import           Control.Monad.Logger        (LogLevel (..), logInfo)

import qualified Data.Configurator           as C


main :: IO ()
main = do
    (conf,_tid) <- C.autoReload C.autoConfig ["/etc/aremi/aemo.conf"]
    connStr <- C.require conf "db-conn-string"
    conns <- C.lookupDefault 10 conf "db-connections"

    execAppM (AS Nothing makeLog LevelInfo) $ do
        withPostgresqlPool connStr conns $ \conn -> do
            connPool ?= conn
            $(logInfo) "Running full update of latest_power_station_datum"
            updateLatestTimesSlow
            $(logInfo) "Completed update."


