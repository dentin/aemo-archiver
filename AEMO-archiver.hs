{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

-- import           Data.List.Split              (chunksOf)
import           System.IO                   (BufferMode (NoBuffering),
                                              hSetBuffering, stdout)

import           Database.Persist.Postgresql

import           AEMO.CSV
import           AEMO.Database
import           AEMO.Types

import           Control.Lens

import Control.Monad.Logger (LogLevel(..))

import           Data.Configurator.Types (Config)
import qualified Data.Configurator as C


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    (conf,_tid) <- C.autoReload C.autoConfig ["/etc/aremi/aemo.conf"]
    connStr <- C.require conf "db-conn-string"
    conns <- C.lookupDefault 10 conf "db-connections"

    execAppM (AS Nothing makeLog LevelInfo) $ do
        withPostgresqlPool connStr conns $ \conn -> do
            connPool ?= conn
            -- Get the names of all known zip files in the database
            knownZipFiles <- allDbZips

            -- fetchDaily5mActualLoad knownZipFiles
            processDailys knownZipFiles

