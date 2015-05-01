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


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    connStr <- dbConn

    execAppM (AS Nothing makeLog LevelInfo) $ do
        withPostgresqlPool connStr 1 $ \conn -> do
            connPool ?= conn
            -- Get the names of all known zip files in the database
            knownZipFiles <- allDbZips

            fetchDaily5mActualLoad knownZipFiles

