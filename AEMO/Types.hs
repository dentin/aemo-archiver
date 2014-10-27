{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

module AEMO.Types where

import           Database.Persist.TH

import           Data.Text           (Text)

import           Data.Time


share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
	PowerStation
		duid Text
		name Text
		UniquePSCode duid
		deriving Show

	AemoCsvFile
		fileName Text
		timeInserted UTCTime default=CURRENT_TIME
		recordsInserted Int
		UniqueAEMOFile fileName

	AemoZipFile
		fileName Text

	PSDatum
		duid Text
		sampleTime UTCTime
		kiloWattHours Double
		file AemoCsvFileId
		deriving Show

	|]
