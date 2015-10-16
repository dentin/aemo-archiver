{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Data.ByteString.Lazy        (ByteString)
import qualified Data.ByteString.Lazy        as B
import qualified Data.Vector                 as V

import           Data.Text                   (Text)

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

#if !MIN_VERSION_base(4,8,0)
import           Control.Applicative
#endif

import qualified Data.Attoparsec.Text        as A

import qualified Data.Configurator           as C

import System.Environment (getArgs)

import Data.List as L


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

            -- Check if we have any power stations and locations, otherwise initialise them
            numLocs <- runDB $ count ([] :: [Filter DuidLocation])
            numStations <- runDB $ count ([] :: [Filter PowerStation])

            args <- liftIO $ getArgs
            -- TODO: Use proper option parsing if we're going to do more of this
            let updateLocs = foldr (\h t -> h == "--update" || t) False args
                dryRun     = foldr (\h t -> "--dry" `L.isPrefixOf` h || t) False args

            when ((numLocs == 0 && numStations == 0) ||  updateLocs) $ do
                (locs,ps) <- liftIO $ do

                    -- load station locs
                    locsExist <- doesFileExist stationLocs
                    unless (locsExist) $ do
                        putStrLn $ "File does not exist: " ++ stationLocs
                        exitWith $ ExitFailure 1
                    locCsv <- B.readFile stationLocs
                    locRows <- either error return $ parseGensAndSchedLoads locCsv

                    print $ V.length locRows
                    V.mapM_ print locRows

                    -- Load station data
                    psExist <- doesFileExist gensAndLoads
                    unless (psExist) $ do
                        putStrLn $ "File does not exist: " ++ gensAndLoads
                        exitWith $ ExitFailure 1
                    stationCsv <- B.readFile gensAndLoads
                    (_hrs,psRows) <- either error return $ decodeByName stationCsv :: IO ((Header, V.Vector PowerStation))
                    -- psRows <- either error return $ decode HasHeader bs :: IO (V.Vector (V.Vector ByteString))
                    print $ V.length psRows
                    V.mapM_ print psRows
                    return (locRows,psRows)

                unless dryRun $ runDB $ do
                    when updateLocs $ do
                        deleteWhere ([] :: [Filter DuidLocation])
                        deleteWhere ([] :: [Filter PowerStation])
                    insertMany_ . map (\(duid, (lat, lon), comm) -> DuidLocation duid lat lon comm)
                                . V.toList
                                $ locs
                    insertMany_ . V.toList $ ps

            return ()


            arcs   <- fetchArchiveActualLoad
            dailys <- fetchDaily5mActualLoad
            when (arcs + dailys > 0)
                refreshLatestDUIDTime


parseGensAndSchedLoads :: ByteString -> Either String (V.Vector (Text, (Double, Double), Text))
parseGensAndSchedLoads bs =  do
    vec <- decode NoHeader bs
    V.mapM prs vec where
        prs (duid,llstr,comm) = do
            ll <- A.parseOnly ((,) <$> A.double <*> (" " *> A.double)) llstr
            return (duid,ll,comm)
