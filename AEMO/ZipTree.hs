{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}

module AEMO.ZipTree where


import           Control.Monad
import           Data.Traversable     hiding (mapM)
import Data.Foldable hiding (elem, concat, concatMap)
import Data.Functor
import Control.Applicative

import           Data.ByteString.Lazy (ByteString)

import           Data.Char            (toLower)
import           Data.List            (isSuffixOf, partition)


import           Codec.Archive.Zip    (filesInArchive, findEntryByPath,
                                       fromEntry, toArchiveOrFail, Archive(..), Entry(..))


data ZipTree a = ZipFile FilePath ByteString
             | File FilePath a
             | Node FilePath [ZipTree a]
    deriving (Functor, Foldable, Traversable)

getFilePath :: ZipTree a -> FilePath
getFilePath (Node n _)    = n
getFilePath (ZipFile n _) = n
getFilePath (File n _)    = n

instance Show (ZipTree a) where
    show (ZipFile fp _) = "ZipFile " ++ show fp
    show (File fp _)    = "File " ++ show fp
    show (Node fp bs)   = "Node " ++ show fp ++ " " ++ showList bs ""

traverseWithName :: (Applicative f) => (FilePath -> a -> f b) -> ZipTree a -> f (ZipTree b)
traverseWithName f (File n a) = File n <$> f n a
traverseWithName f (Node n as) = Node n <$> (sequenceA . map (traverseWithName f) $ as)
traverseWithName _ (ZipFile n bs) = pure (ZipFile n bs)


travseseWithParentAndName :: Monad f
                          => (FilePath -> f c)
                          -> (FilePath -> f ())
                          -> (Maybe c -> FilePath -> a -> f b)
                          -> ZipTree a
                          -> f (ZipTree b)
travseseWithParentAndName nod zp fl ziptree = go Nothing ziptree where
    go _      (Node n ls)    = nod n >>= \c -> liftM (Node n) $ mapM (go (Just c)) ls
    go _      (ZipFile n bs) = zp n  >> return (ZipFile n bs)
    go parent (File n a)     = liftM (File n) $ fl parent n a


infix 3 ?
(?) :: Maybe a -> String -> Either String a
m ? s = maybe (Left s) Right m

infix 3 ??
(??) :: Either String a -> String -> Either String a
e ?? s = either (Left . (s ++)) Right e


toZipTree :: Int -> (FilePath,ByteString) -> Either String (ZipTree ByteString)
toZipTree 0 (filepath,bs) = do
    arc <- toArchiveOrFail bs
    let filenames = filesInArchive arc
        (zips,files) = partition (isSuffixOf ".zip" . map toLower) filenames
    zipTrees  <- mapM (toLeaf ZipFile arc) zips
    fileTrees <- mapM (toLeaf File    arc) files
    return $ Node filepath $ fileTrees ++ zipTrees

toZipTree n (filepath,bs) = do
    arc <- toArchiveOrFail bs
    let filenames = filesInArchive arc
        (zips,files) = partition (isSuffixOf ".zip" . map toLower) filenames
        -- zipEnts  = mapM (toLeaf ZipFile arc) zips
    fileTrees <- mapM (toLeaf File    arc) files
    zipTrees  <- mapM (toZipTree (n-1) <=< fpBsPair arc) zips
    return $ Node filepath $ fileTrees ++ zipTrees
    where
        fpBsPair :: Archive -> FilePath -> Either String (FilePath, ByteString)
        fpBsPair arc fp = do
            ent <- findEntryByPath fp arc ? "Could not find path \"" ++ fp ++ "\" in archive \"" ++ filepath ++ "\""
            return (eRelativePath ent, fromEntry ent)

toLeaf :: (FilePath -> ByteString -> (ZipTree ByteString)) -> Archive -> FilePath -> Either String (ZipTree ByteString)
toLeaf leaf arc path = do
    ent <- findEntryByPath path arc ? "Could not find path \"" ++ path ++ "\" in archive"
    return $ leaf (eRelativePath ent) (fromEntry ent)


