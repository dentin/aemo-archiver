{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}


module AEMO.CSV
    ( processDailys
    , processArchives
    ) where

import           Data.ByteString.Lazy        (ByteString)
-- import qualified Data.ByteString.Lazy        as B
import           Data.Vector                 (Vector)
import qualified Data.Vector                 as V
-- TODO: why Char8?
import           Control.Monad               (when)
import Data.Functor
import qualified Data.ByteString.Lazy.Char8  as C (intercalate, lines)
import           Data.Csv                    (HasHeader (..), decode)
import           Data.Either                 (partitionEithers)
import qualified Data.HashSet                as S (fromList, member)
import           Data.Text                   (Text)
import qualified Data.Text                   as T (pack)
import Data.Maybe (isJust)

import           Database.Persist.Postgresql

import           Control.Monad.Logger

import           AEMO.Database
import           AEMO.Types
import           AEMO.WebScraper

import           Data.Conduit (Sink, Conduit, (=$=), ($$))
import qualified Data.Conduit as C
import           Data.Conduit.List ()
import qualified Data.Conduit.List as CL
import Control.Monad.Trans

import Control.Arrow (first)


{-Notes to self:
    - Currently in the process of tagging things with newtypes to show what sorts of String
      we're working with
    - Some how need to keep track of all zip and csv file names while maintaining the same API
      for both invocations of extractFiles - perhaps provide an accessor fiunction to get the
      relevant parts of the tuple being passed in
    - The "pipeline" is probably only relevant for each zip file download - do all other processing
      in standard monadic code
-}

processDailys :: [Text] -> AppM ()
processDailys knownZips = do
    let seenfiles = S.fromList knownZips
    -- liftIO $ print seenfiles
    getARefs aemo5mPSURL $$
        joinLinks aemo5mPSURL  -- (FileName,URL)
        =$= CL.map (first ZipName) -- (ZipName,URL)
        =$= filterM' (\(fn,_) -> not <$> (liftIO (print fn) >> knownZIP fn)) -- (FileName,URL)
        =$= fetchFiles -- (FileName,ByteString)
        =$= extractFiles ".csv" -- ((ZipName,FileName), ByteString) -- CSV
        =$= CL.map (\((zp,fn),bs) -> ((zp,CSVName fn), bs))
        =$= processZips

processArchives :: [Text] -> AppM ()
processArchives knownZips = do
    let seenfiles = S.fromList knownZips
    getARefs aemoPSArchiveURL $$
        joinLinks aemoPSArchiveURL  -- (FileName,URL)
        =$= CL.map (\(fn,url) -> (ZipName fn, url))
        -- =$= CL.filter (\(fn,_) -> not $ S.member (T.pack fn) seenfiles) -- (FileName,URL)
        =$= filterM' (\(fn,_) -> not <$> (liftIO (print fn) >> knownZIP fn)) -- (FileName,URL)
        =$= fetchFiles -- (FileName,ByteString)
        =$= extractFiles ".zip" -- ((ZipName,FileName), ByteString) -- ZIP
        -- =$= filterM' (\((_zn,csvn),_) -> not <$> knownCSV csvn)
        =$= CL.map (\((zn,_fn),bs) -> (zn,bs)) -- (ZipName,ByteString) -- ZIP
        =$= extractFiles ".csv" -- ((ZipName,FileName), ByteString) -- CSV
        =$= processZips


knownCSV :: CSVName -> AppM Bool
knownCSV (CSVName f) = runDB $ isJust <$>  selectFirst [AemoCsvFileFileName ==. T.pack f] []

knownZIP :: ZipName -> AppM Bool
knownZIP (ZipName f) = runDB $ isJust <$>  selectFirst [AemoZipFileFileName ==. T.pack f] []

filterM' :: Monad m => (a -> m Bool) -> Conduit a m a
filterM' p = C.awaitForever $ \x -> do
    keep <- lift (p x)
    when keep $ C.yield x



processZips :: Sink ((ZipName,CSVName), ByteString) AppM ()
processZips = C.awaitForever $ \((zp,fn),csv) -> do
        notInDB <- lift $ csvNotInDb fn
        when notInDB $ case parseAEMO csv of
            Left err -> $(logWarn) $ T.pack (concat ["processZips: parseAEMO failed on ", fn, " from ", zp,": ",err])
            Right parsed -> lift $ runDB $ do -- Vector CSVRow
                insertCSV (fn, parsed)
                insert $ AemoZipFile (T.pack zp)
                return ()



insertCSV :: (FileName, Vector CSVRow) -> DBMonad ()
insertCSV (file, vec) = do
    fid <- insert $ AemoCsvFile (T.pack file) (V.length vec)
    let (errs,psdata) = partitionEithers . map (csvTupleToPowerStationDatum fid) . V.toList $ vec
    case errs of
        [] -> do
            insertMany_  psdata
            $(logInfo) $ T.pack $ "Inserted data from " ++ file
        _  -> do
            $(logError) "Failed to parse CSV"
            mapM_ ($(logError) . T.pack . show) errs


-- | Parse the AEMO CSV files which contain daily data
parseAEMO :: ByteString -> Either String (Vector CSVRow)
parseAEMO file =
    -- Removes the beginning and end lines of the file
    -- AEMO data files have two headers and a footer which causes issues when parsing
    -- Using intercalate instead of unlines to ensure that new lines are the same
    -- as the original document
    let trimmed = C.intercalate "\r\n" . init . drop 1 . C.lines $ file
    in  decode HasHeader trimmed
