{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Data.ByteString.Lazy        (ByteString)
import qualified Data.ByteString.Lazy        as B
import           Data.Vector                 (Vector)
import qualified Data.Vector                 as V

import           Data.Text                   (Text)
import qualified Data.Text                   as T

import           Data.Csv                    (HasHeader (..), Header, decode,
                                              decodeByName)



import           Control.Monad               (unless, when)

import           System.Directory            (doesFileExist)
import           System.Exit                 (ExitCode (ExitFailure), exitWith)
import           System.IO                   (BufferMode (NoBuffering),
                                              hSetBuffering, stdout)

import           Control.Monad.IO.Class      (liftIO)

import           Database.Persist.Postgresql

import           AEMO.CSV
import           AEMO.Database
import           AEMO.Types

import           Control.Lens

import           Control.Monad.Logger        (LogLevel (..))

import           Control.Applicative
import qualified Data.Attoparsec.Text        as A
import qualified Data.Vector                 as V


import           Data.Configurator.Types (Config)
import qualified Data.Configurator as C


gensAndLoads :: FilePath
gensAndLoads = "power_station_metadata/nem-Generators and Scheduled Loads.csv"

stationLocs :: FilePath
stationLocs = "power_station_metadata/power_station_locations.csv"


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    (conf,_tid) <- C.autoReload C.autoConfig ["/etc/aremi/aemo.conf"]
    connStr <- C.require conf "db-conn-string"
    conns <- C.lookupDefault 10 conf "db-connections"

    execAppM (AS Nothing makeLog LevelInfo) $ do
        withPostgresqlPool connStr conns $ \conn -> do
            connPool ?= conn

            migrateDb

            numLocs <- runDB $ count ([] :: [Filter DuidLocation])
            when (numLocs == 0) $ do
                locs <- liftIO $ do
                    exists <- doesFileExist stationLocs
                    unless (exists) $ do
                        putStrLn $ "File does not exist: " ++ stationLocs
                        exitWith $ ExitFailure 1
                    bs <- B.readFile stationLocs
                    rows <- either error return $ parseGensAndSchedLoads bs

                    print $ V.length rows
                    V.mapM_ print rows
                    return rows

                runDB $ do
                    insertMany_ . map (\(duid, (lat, lon), comm) -> DuidLocation duid lat lon comm)
                                . V.toList
                                $ locs
            return ()

                        -- Check if we have any power stations, otherwise initialise them
            numStations <- runDB $ count ([] :: [Filter PowerStation])
            when (numStations == 0) $ do
                ps <- liftIO $ do
                    exists <- doesFileExist gensAndLoads
                    unless (exists) $ do
                        putStrLn $ "File does not exist: " ++ gensAndLoads
                        exitWith $ ExitFailure 1
                    bs <- B.readFile gensAndLoads
                    (hrs,rows) <- either error return $ decodeByName bs :: IO ((Header, V.Vector PowerStation))
                    -- rows <- either error return $ decode HasHeader bs :: IO (V.Vector (V.Vector ByteString))
                    print $ V.length rows
                    V.mapM_ print rows
                    return rows

                runDB . insertMany_ . V.toList $ ps
            return ()


            -- Get the names of all known zip files in the database
            knownZipFiles <- allDbZips
            -- fetchArchiveActualLoad knownZipFiles
            processArchives knownZipFiles

            -- Run this twice because the previous call adds many new zip files
            knownZipFiles <- allDbZips
            -- fetchDaily5mActualLoad knownZipFiles
            processDailys knownZipFiles


parseGensAndSchedLoads :: ByteString -> Either String (V.Vector (Text, (Double, Double), Text))
parseGensAndSchedLoads bs =  do
    vec <- decode NoHeader bs
    V.mapM prs vec where
        prs (duid,llstr,comm) = do
            ll <- A.parseOnly ((,) <$> A.double <*> (" " *> A.double)) llstr
            return (duid,ll,comm)
