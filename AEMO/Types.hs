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

import           Database.Persist.TH (mkPersist, sqlSettings, mkMigrate, persistLowerCase, share)
import           Data.Text           (Text)
import           Data.Time 			 (UTCTime)


share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
	PowerStation
		participant				Text
		stationName				Text
		region					Text
		dispatchType			Text
		category				Text
		classification			Text
		fuelSourcePrimary		Text Maybe
		fuelSourceDescriptor	Text Maybe
		techTypePrimary			Text Maybe
		techTypeDescriptor		Text Maybe
		physicalUnitNo			Text Maybe
		unitSizeMW				Text
		aggregation				Bool
		duid					Text
		regCapMW				Double Maybe
		maxCapMW				Double Maybe
		maxROCPerMin			Double Maybe

	AemoCsvFile
		fileName Text
		recordsInserted Int
		UniqueAEMOFile fileName

	AemoZipFile
		fileName Text

	PSDatum
		duid Text
		sampleTime UTCTime
		megaWatt Double
		file AemoCsvFileId
		deriving Show

	|]
