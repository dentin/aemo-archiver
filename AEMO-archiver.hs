{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

-- import           Data.List.Split              (chunksOf)
import           System.IO                   (BufferMode (NoBuffering),
                                              hSetBuffering, stdout)

import           Control.Monad.Logger

import           Database.Persist.Postgresql

import           AEMO.Database
import           AEMO.Types
import AEMO.CSV


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering

    runNoLoggingT $ do
        withPostgresqlPool dbConn 10 $ \conn ->
            execLoggerAppM (AS conn) $ do
                -- Get the names of all known zip files in the database
                knownZipFiles <- allDbZips

                fetchDaily5mActualLoad knownZipFiles

