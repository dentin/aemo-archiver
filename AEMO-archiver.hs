{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

-- import           Data.List.Split              (chunksOf)
import           System.IO                   (BufferMode (NoBuffering),
                                              hSetBuffering, stdout)

import           Database.Persist.Postgresql

import           AEMO.CSV
import           AEMO.Types
import           AEMO.Database (refreshLatestDUIDTime)

import Control.Monad (when)

import           Control.Lens

import Control.Monad.Logger (LogLevel(..))

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
            arcs   <- fetchArchiveActualLoad
            dailys <- fetchDaily5mActualLoad
            when (arcs + dailys > 0)
                refreshLatestDUIDTime

