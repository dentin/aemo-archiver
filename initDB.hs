{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Data.ByteString.Char8       as C8
import           Data.ByteString.Lazy        (ByteString)
import qualified Data.ByteString.Lazy        as B
import qualified Data.ByteString.Lazy.Char8  as B8
import qualified Data.Vector                 as V

import           Data.Text                   (Text, pack, unpack)
import qualified Data.Text                   as T

import qualified Data.Map.Strict             as M


import           Data.Csv                    (HasHeader (..), Header, decode,
                                              decodeByName, encodeByName)



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

import           Text.Read

import           Data.Function               (on)

import           Data.Scientific

import           Configuration.Utils         hiding (decode)
import           PkgInfo_initDB

import qualified Network.Wreq                as Wreq

mainInfo :: ProgramInfo AEMOConf
mainInfo = programInfo "initDB" pAEMOConf defaultAemoConfig

main :: IO ()
main = runWithPkgInfoConfiguration mainInfo pkgInfo $ \conf -> do
    hSetBuffering stdout NoBuffering
    let conns   = conf ^. aemoDB . dbConnections
        connStr = C8.pack $ conf ^. aemoDB . dbConnString

    execAppM (AS Nothing makeLog LevelInfo Wreq.defaults) $
        withPostgresqlPool connStr conns $ \conn -> do
            connPool ?= conn

            let updateLocs = _aemoUpdateStations conf
                dryRun     = _aemoDryRun conf

            unless dryRun migrateDb

            -- Check if we have any power stations and locations, otherwise initialise them
            numLocs <- runDB $ count ([] :: [Filter DuidLocation])
            numStations <- runDB $ count ([] :: [Filter PowerStation])

            when ((numLocs == 0 && numStations == 0) ||  updateLocs) $ do
                (locs,ps) <- liftIO $ do

                    -- load station locs
                    let stationLocs = _aemoStationLocs conf
                    locsExist <- doesFileExist stationLocs
                    unless (locsExist) $ do
                        putStrLn $ "File does not exist: " ++ stationLocs
                        exitWith $ ExitFailure 1
                    locCsv <- B.readFile stationLocs
                    locRows <- either error return $ parseGensAndSchedLoads locCsv

                    -- print $ V.length locRows
                    -- V.mapM_ print locRows

                    -- Load station data
                    let gensAndLoads = _aemoGensAndLoads conf
                    psExist <- doesFileExist gensAndLoads
                    unless psExist $ do
                        putStrLn $ "File does not exist: " ++ gensAndLoads
                        exitWith $ ExitFailure 1
                    stationCsv <- B.readFile gensAndLoads
                    (_hrs,psRows) <- either error return $ decodeByName stationCsv :: IO (Header, V.Vector PowerStation)
                    -- psRows <- either error return $ decode HasHeader bs :: IO (V.Vector (V.Vector ByteString))
                    -- print $ V.length psRows
                    let cleanPows = map snd
                             . M.toList
                             . M.delete "-"
                             . M.delete " - "
                             . M.fromListWith (\a b -> either error id $ joinPowerStationRows a b)
                             . map (\p -> (powerStationDuid p, p))
                             . V.toList
                             $ psRows

                    -- mapM_ print cleanPows
                    B8.putStrLn $ encodeByName
                        ["Participant"
                        ,"Station Name"
                        ,"Region"
                        ,"Dispatch Type"
                        ,"Category"
                        ,"Classification"
                        ,"Fuel Source - Primary"
                        ,"Fuel Source - Descriptor"
                        ,"Technology Type - Primary"
                        ,"Technology Type - Descriptor"
                        ,"Physical Unit No."
                        ,"Unit Size (MW)"
                        ,"Aggregation"
                        ,"DUID"
                        ,"Reg Cap (MW)"
                        ,"Max Cap (MW)"
                        ,"Max ROC/Min"
                        ]
                        cleanPows
                    return (locRows,cleanPows)

                unless dryRun $ runDB $
                    when updateLocs $ do
                        deleteWhere ([] :: [Filter DuidLocation])
                        insertMany_ . V.toList $ locs

                        deleteWhere ([] :: [Filter PowerStation])
                        insertMany_  ps

            unless dryRun $ do
                _ <- fetchArchiveActualLoad
                _ <- fetchDaily5mActualLoad
                pure ()




joinPowerStationRows :: PowerStation -> PowerStation -> Either String PowerStation
joinPowerStationRows p1 p2 = PowerStation
    <$> joinOn powerStationParticipant              p1 p2
    <*> joinOn powerStationStationName              p1 p2
    <*> joinOn powerStationRegion                   p1 p2
    <*> joinOn powerStationDispatchType             p1 p2
    <*> joinOn powerStationCategory                 p1 p2
    <*> joinOn powerStationClassification           p1 p2
    <*> joinOnM powerStationFuelSourcePrimary       p1 p2
    <*> joinOnM powerStationFuelSourceDescriptor    p1 p2
    <*> joinOnM powerStationTechTypePrimary         p1 p2
    <*> joinOnM powerStationTechTypeDescriptor      p1 p2
    <*> joinOnM' powerStationPhysicalUnitNo         p1 p2
    <*> joinOn powerStationUnitSizeMW               p1 p2
    <*> pure (((||) `on` powerStationAggregation)   p1 p2)
    <*> (pure $ powerStationDuid                    p1)     -- Should be guaranteed to be the same or something went very wrong
    <*> sumText powerStationRegCapMW                p1 p2
    <*> sumText powerStationMaxCapMW                p1 p2
    <*> joinOnM powerStationMaxROCPerMin            p1 p2

-- | Adds two strings as numbers if it can parse them
sumText :: (PowerStation -> Maybe Text) -> PowerStation -> PowerStation -> Either String (Maybe Text)
sumText f p1 p2 = either (\e -> Left (unlines ["sumText: " ++ e,show p1, show p2])) Right $
    case (f' p1, f' p2) of
        (Just a, Just b) -> either (Left . (show (a,b) ++)) Right $ do
            an <- readEither a <|> Right 0
            bn <- readEither b <|> Right 0
            pure . Just . pack . show $ (an+bn :: Scientific)
        (a,b) -> pure . fmap pack $ a <|> b

    where f' = fmap unpack . f

joinOnM :: (PowerStation -> Maybe Text) -> PowerStation -> PowerStation -> Either String (Maybe Text)
joinOnM f p1 p2 = pure $ (joinText <$> m1 <*> m2) <|> m1 <|> m2
    where
        m1 = f p1
        m2 = f p2

joinOnM' :: (PowerStation -> Maybe Text) -> PowerStation -> PowerStation -> Either String (Maybe Text)
joinOnM' f p1 p2 = pure $ (joinText' <$> m1 <*> m2) <|> m1 <|> m2
    where
        m1 = f p1
        m2 = f p2

joinOn :: (PowerStation -> Text) -> PowerStation -> PowerStation -> Either String Text
joinOn f p1 p2 = pure $ joinText (f p1) (f p2)


joinText :: Text -> Text -> Text
joinText a b
    | a == b    = a
    | otherwise = T.concat [b,", ", a]

joinText' :: Text -> Text -> Text
joinText' a b = T.concat [b,", ", a]
-- joinWith :: (PowerStation -> PowerStation -> Text) -> Maybe  -> Maybe a -> Maybe b
-- joinWith f (Just t) (Just b)

parseGensAndSchedLoads :: ByteString -> Either String (V.Vector DuidLocation)
parseGensAndSchedLoads bs =  do
    vec <- decode NoHeader bs
    V.mapM prs vec where
        prs (duid,llstr,comm) = do
            (lat,lon) <- A.parseOnly ((,) <$> A.double <*> (" " *> A.double)) llstr
            return $ DuidLocation (T.strip duid) lat lon (T.strip comm)
