module Main where

import           Data.ByteString.Lazy        (ByteString)
import qualified Data.ByteString.Lazy        as B
import           Data.Vector                 (Vector)

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


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering

    -- runNoLoggingT $ do
    execAppM (AS Nothing makeLog LevelDebug) $ do
        withPostgresqlPool dbConn 10 $ \conn -> do
            connPool ?= conn

            migrateDb

            -- Check if we have any power stations, otherwise initialise them
            numStations <- runDB $ count ([] :: [Filter PowerStation])
            when (numStations == 0) $ liftIO $ do
                exists <- doesFileExist gensAndLoads
                unless (exists) $ do
                    putStrLn $ "File does not exist: " ++ gensAndLoads
                    exitWith $ ExitFailure 1
                bs <- B.readFile gensAndLoads
                -- either error (undefined) $ parseGensAndSchedLoads bs
                print $ B.length bs


            -- Get the names of all known zip files in the database
            knownZipFiles <- allDbZips

            fetchArchiveActualLoad knownZipFiles
            fetchDaily5mActualLoad knownZipFiles


parseGensAndSchedLoads :: ByteString -> Either String (Vector CSVRow)
parseGensAndSchedLoads _bs = undefined
