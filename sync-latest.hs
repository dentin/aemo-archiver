{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Main where


import           Database.Persist.Postgresql

import           AEMO.Database
import           AEMO.Types

import           Control.Lens

import           Control.Monad.Logger        (LogLevel (..), logInfo)

import           System.IO                   (BufferMode (NoBuffering),
                                              hSetBuffering, stdout)
                                              
import qualified Data.ByteString.Char8       as C8

import Configuration.Utils hiding (decode)
import PkgInfo_sync_latest

mainInfo :: ProgramInfo AEMOConf
mainInfo = programInfo "sync-latest" pAEMOConf defaultAemoConfig

main :: IO ()
main = runWithPkgInfoConfiguration mainInfo pkgInfo $ \conf -> do
    hSetBuffering stdout NoBuffering
    let conns   = _aemoDBCons conf
        connStr = C8.pack $ _aemoDBString conf

    execAppM (AS Nothing makeLog LevelInfo) $ do
        withPostgresqlPool connStr conns $ \conn -> do
            connPool ?= conn
            $(logInfo) "Running full update of latest_power_station_datum"
            updateLatestTimesSlow
            $(logInfo) "Completed update."
