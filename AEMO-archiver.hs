{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

-- import           Data.List.Split              (chunksOf)
import           System.IO                   (BufferMode (NoBuffering),
                                              hSetBuffering, stdout)

import           Database.Persist.Postgresql

import           AEMO.CSV
import           AEMO.Types

import           Control.Lens

import           Control.Monad.Logger        (LogLevel (..))

import qualified Data.ByteString.Char8       as C8

import           Configuration.Utils         hiding (decode)
import           PkgInfo_aemo_archiver

import qualified Network.Wreq as Wreq

mainInfo :: ProgramInfo AEMOConf
mainInfo = programInfo "aemo-archiver" pAEMOConf defaultAemoConfig

main :: IO ()
main = runWithPkgInfoConfiguration mainInfo pkgInfo $ \conf -> do
    hSetBuffering stdout NoBuffering
    let conns   = conf ^. aemoDB . dbConnections
        connStr = C8.pack $ conf ^. aemoDB . dbConnString

    execAppM (AS Nothing makeLog LevelInfo Wreq.defaults) $ do
        withPostgresqlPool connStr conns $ \conn -> do
            connPool ?= conn
            _ <- fetchArchiveActualLoad
            _ <- fetchDaily5mActualLoad
            pure ()
