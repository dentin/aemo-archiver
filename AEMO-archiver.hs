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

mainInfo :: ProgramInfo AEMOConf
mainInfo = programInfo "aemo-archiver" pAEMOConf defaultAemoConfig

main :: IO ()
main = runWithPkgInfoConfiguration mainInfo pkgInfo $ \conf -> do
    hSetBuffering stdout NoBuffering
    let conns   = _aemoDBCons conf
        connStr = C8.pack $ _aemoDBString conf

    execAppM (AS Nothing makeLog LevelInfo) $ do
        withPostgresqlPool connStr conns $ \conn -> do
            connPool ?= conn
            _ <- fetchArchiveActualLoad
            _ <- fetchDaily5mActualLoad
            pure ()
