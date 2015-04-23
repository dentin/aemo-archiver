module Main where

import           Data.ByteString.Lazy        (ByteString)
import qualified Data.ByteString.Lazy        as B
import           Data.Vector (Vector)
import qualified Data.Vector as V

import           Data.Text (Text)
import qualified Data.Text as T

import Data.Csv (decode, HasHeader(..))



import           Control.Monad               (unless, when)

import           System.Directory            (doesFileExist)
import           System.Exit                 (ExitCode (ExitFailure), exitWith)
import           System.IO                   (BufferMode (NoBuffering),
                                              hSetBuffering, stdout)

import           Control.Monad.IO.Class      (liftIO)

import           Database.Persist.Postgresql

import           AEMO.Database
import           AEMO.Types
import AEMO.CSV

import           Control.Lens

import Control.Monad.Logger (LogLevel(..))


gensAndLoads :: FilePath
gensAndLoads = "nem-Generators and Scheduled Loads.csv"

stationLocs :: FilePath
stationLocs = "power_station_locations.csv"


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering

    -- runNoLoggingT $ do
    execAppM (AS Nothing makeLog LevelDebug) $ do
        withPostgresqlPool dbConn 1 $ \conn -> do
            connPool ?= conn

            migrateDb

            -- Check if we have any power stations, otherwise initialise them
            -- numStations <- runDB $ count ([] :: [Filter PowerStation])
            -- when (numStations == 0) $
            liftIO $ do
                exists <- doesFileExist stationLocs
                unless (exists) $ do
                    putStrLn $ "File does not exist: " ++ stationLocs
                    exitWith $ ExitFailure 1
                bs <- B.readFile stationLocs
                rows <- either error return $ parseGensAndSchedLoads bs

                print $ V.length rows
                V.mapM_ print rows
                -- runDB $ do
                    -- insertMany_ $ map ()


            -- Get the names of all known zip files in the database
            -- knownZipFiles <- allDbZips

            -- fetchArchiveActualLoad knownZipFiles
            -- fetchDaily5mActualLoad knownZipFiles


parseGensAndSchedLoads :: ByteString -> Either String (Vector (Text, Text, Text))
parseGensAndSchedLoads bs =  decode NoHeader bs
