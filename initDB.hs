module Main where

import           Data.ByteString.Lazy        (ByteString)
import qualified Data.ByteString.Lazy        as B
import           Data.Vector                 (Vector)

import           Control.Arrow               (second)
import           Control.Monad               (filterM, unless, when)
import qualified Data.ByteString.Lazy.Char8  as C (intercalate, lines)
import           Data.Csv                    (HasHeader (..), decode)
import           Data.Either                 (partitionEithers)
import qualified Data.HashSet                as S (fromList, member)
import           Data.Text                   (Text)
import qualified Data.Text                   as T (pack)

import           System.Directory            (doesFileExist)
import           System.Exit                 (ExitCode (ExitFailure), exitWith)
import           System.IO                   (BufferMode (NoBuffering),
                                              hSetBuffering, stdout)

import           Control.Monad.IO.Class      (liftIO)
import           Control.Monad.Logger

import           Database.Persist.Postgresql

import           AEMO.Database
import           AEMO.Types
import           AEMO.WebScraper



gensAndLoads :: FilePath
gensAndLoads = "nem-Generators and Scheduled Loads.csv"


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering

    runNoLoggingT $ do
        withPostgresqlPool dbConn 10 $ \conn ->
            execLoggerAppM (AS conn) $ do

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

                return ()

                -- fetchArchiveActualLoad knownZipFiles
                -- fetchDaily5mActualLoad knownZipFiles