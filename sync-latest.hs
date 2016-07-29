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

import           Configuration.Utils         hiding (decode)
import           PkgInfo_sync_latest

import qualified Network.Wreq as Wreq

mainInfo :: ProgramInfo AEMOConf
mainInfo = programInfo "sync-latest" pAEMOConf defaultAemoConfig

main :: IO ()
main = runWithPkgInfoConfiguration mainInfo pkgInfo $ \conf -> do
    hSetBuffering stdout NoBuffering
    let conns   = conf ^. aemoDB . dbConnections
        connStr = C8.pack $ conf ^. aemoDB . dbConnString

    execAppM (AS Nothing makeLog LevelInfo Wreq.defaults) $ do
        withPostgresqlPool connStr conns $ \conn -> do
            connPool ?= conn
            $(logInfo) "Running full update of latest_power_station_datum"
            updateLatestTimesSlow
            $(logInfo) "Completed update."
