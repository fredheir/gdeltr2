#' Loads needed packages
#'
#' @param required_packages
#'
#' @return
#' @export
#'
#' @examples
#' load_neeeded_packages(c('magrittr', 'dplyr))
load_needed_packages <- function(required_packages = c('dplyr')) {
  loaded_packages <- gsub("package:", "", search())
  package_to_load <- required_packages[!required_packages %in%
                                         loaded_packages]
  if (length(package_to_load) > 0) {
    lapply(package_to_load, library, character.only = T)
  }
}

#' Loads gdelt v2 Global Knowledge Graph master log data, updated every 15 minutes
#'
#' @return
#' @export
#' @import dplyr
#' @import stringr
#' @importFrom lubridate ymd_hms
#' @importFrom lubridate with_tz
#' @importFrom readr read_tsv
#' @importFrom magrittr %>%
#' @importFrom tidyr separate
#' @examples
#' get_urls_gkg_15_minute_log()
get_urls_gkg_15_minute_log <- function() {

  url <-
    'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'

  log_df <-
    url %>%
    readr::read_tsv(col_names = F) %>%
    tidyr::separate(
      col = X1,
      into = c('idFile', 'idHash', 'urlData'),
      sep = '\\ '
    ) %>%
    suppressWarnings()

  log_df <-
    log_df %>%
    dplyr::mutate(
      date_timeFile = urlData %>% str_replace_all('http://data.gdeltproject.org/gdeltv2/', '')
    ) %>%
    tidyr::separate(date_timeFile,
                    into = c('timestamp', 'nameFile', 'typeFile', 'isZip'))

  log_df <-
    log_df %>%
    dplyr::mutate(
      date_timeData = timestamp %>% as.numeric %>% ymd_hms() %>% with_tz(Sys.timezone()),
      dateData = date_timeData %>% as.Date(),
      typeFile = typeFile %>% str_to_lower(),
      idFile = idFile %>% as.integer()
    ) %>%
    dplyr::mutate_each_(funs(str_trim), c('idHash', 'nameFile', 'urlData')) %>%
    suppressWarnings()

  return(log_df)

}

#' Gets GDELT Event data, by year from 1979-2005, by year month 2006 - 2013, then by dat
#'
#' @param return_message
#'
#' @return
#' @import stringr
#' @import dplyr
#' @importFrom magrittr %>%
#' @importFrom readr read_tsv
#' @importFrom tidyr separate
#' @importFrom lubridate ymd
#' @export
#'
#' @examples
#' get_urls_gdelt_event_log()
get_urls_gdelt_event_log <- function(return_message = T) {
  url <-
    'http://data.gdeltproject.org/events/md5sums'

  urlData <-
    url %>%
    readr::read_tsv(col_names = F) %>%
    tidyr::separate(
      col = X1,
      into = c('idHash', 'stemData'),
      sep = '\\  '
    ) %>%
    dplyr::mutate(
      urlData = 'http://data.gdeltproject.org/events/' %>% paste0(stemData),
      idDatabaseGDELT = 'EVENTS',
      isZipFile = ifelse(stemData %>% str_detect(".zip"), T, F)
    )

  urlData <-
    urlData %>%
    separate(
      col = stemData,
      into = c('periodData', 'nameFile', 'typeFile', 'zip_file'),
      sep = '\\.'
    ) %>%
    dplyr::select(-zip_file) %>%
    dplyr::mutate(
      periodData = ifelse(periodData == 'GDELT', typeFile, periodData),
      isDaysData = ifelse(periodData %>% nchar == 8, T, F)
    ) %>%
    dplyr::select(-c(nameFile, typeFile)) %>%
    suppressWarnings()

  urlData <-
    urlData %>%
    dplyr::filter(isDaysData == F) %>%
    dplyr::mutate(dateData = NA) %>%
    bind_rows(
      urlData %>%
        dplyr::filter(isDaysData == T) %>%
        dplyr::mutate(dateData = periodData %>% ymd %>% as.Date())
    ) %>%
    dplyr::select(idHash,
                  dateData,
                  isZipFile,
                  isDaysData,
                  urlData,
                  everything())

  if (return_message == T) {
    count.files <-
      urlData %>%
      nrow

    min.date <-
      urlData$dateData %>% min(na.rm = T)

    max.date <-
      urlData$dateData %>% max(na.rm = T)

    "You got " %>%
      paste0(count.files,
             ' GDELT Global Knowledge Graph URLS from ',
             min.date,
             ' to ',
             max.date) %>%
      message()
  }

  return(urlData)
}

#' Gets most recent GKG log URLs
#'
#' @return
#' @import stringr
#' @import dplyr
#' @importFrom magrittr %>%
#' @importFrom readr read_tsv
#' @importFrom tidyr separate
#' @export
#'
#' @examples get_urls_gkg_most_recent_log()
get_urls_gkg_most_recent_log <- function() {
  log_df <-
    'http://data.gdeltproject.org/gdeltv2/lastupdate.txt' %>%
    readr::read_tsv(col_names = F) %>%
    tidyr::separate(
      col = X1,
      into = c('idFile', 'idHash', 'urlData'),
      sep = '\\ '
    )

  log_df %<>%
    dplyr::mutate(
      date_timeFile = urlData %>% str_replace_all('http://data.gdeltproject.org/gdeltv2/', '')
    ) %>%
    tidyr::separate(date_timeFile,
                    into = c('timestamp', 'nameFile', 'typeFile', 'isZip')) %>%
    dplyr::mutate(
      typeFile = typeFile %>% str_to_lower(),
      isZip = ifelse(isZip %>% str_detect("ZIP|zip"), T, F),
      idFile = idFile %>% as.integer()
    ) %>%
    dplyr::mutate_each_(funs(str_trim), c('idHash', 'nameFile', 'urlData'))

  return(log_df)
}


#' Gets Global Knowledge Graph summary files by day since April 2013
#'
#' @param remove_count_files
#' @param return_message
#'
#' @return
#' @export
#' @import stringr
#' @import dplyr
#' @importFrom magrittr %>%
#' @importFrom readr read_tsv
#' @importFrom lubridate ymd
#' @importFrom tidyr separate
#' @examples
#' get_urls_gkg_daily_summaries(remove_count_files = T)
get_urls_gkg_daily_summaries <-
  function(remove_count_files = F,
           return_message = T) {
    url <-
      'http://data.gdeltproject.org/gkg/md5sums'

    urlData <-
      url %>%
      readr::read_tsv(col_names = F) %>%
      tidyr::separate(
        col = X1,
        into = c('idHash', 'stemData'),
        sep = '\\  '
      ) %>%
      dplyr::mutate(urlData = 'http://data.gdeltproject.org/gkg/' %>% paste0(stemData),
                    idDatabaseGDELT = 'gkg') %>%
      separate(
        col = stemData,
        into = c('dateData', 'nameFile', 'typeFile', 'isZipFile'),
        sep = '\\.'
      ) %>%
      dplyr::mutate(
        isZipFile = ifelse(isZipFile == "zip", T, F),
        isCountFile = ifelse(nameFile == 'gkgcounts', T, F),
        dateData = dateData %>% ymd %>% as.Date()
      ) %>%
      dplyr::select(-c(nameFile, typeFile)) %>%
      dplyr::select(idHash,
                    dateData,
                    isZipFile,
                    isCountFile,
                    urlData,
                    everything())

    if (remove_count_files == T) {
      urlData <-
        urlData %>%
        dplyr::filter(isCountFile == F)
    }

    if (return_message == T) {
      count.files <-
        urlData %>%
        nrow

      min.date <-
        urlData$dateData %>% min(na.rm = T)

      max.date <-
        urlData$dateData %>% max(na.rm = T)

      "You got " %>%
        paste0(count.files,
               ' GDELT Global Knowledge Graph URLS from ',
               min.date,
               ' to ',
               max.date) %>%
        message()
    }

    return(urlData)
  }

#' Retrives most recent GDELT Global Content Analysis Measures (GCAM) code book
#'
#'
#' @return
#' @export
#' @importFrom magrittr %>%
#' @importFrom readr read_tsv
#' @examples
#' get_codes_gcam()
get_codes_gcam <- function() {
  url <-
    'http://data.gdeltproject.org/documentation/GCAM-MASTER-CODEBOOK.TXT'
  gcam_data <-
    url %>%
    read_tsv()

  names(gcam_data) <-
    c(
      'idGCAM',
      'idDictionary',
      'idDimension',
      'typeDictionary',
      'codeLanguage',
      'dictionaryHumanName',
      'dimensionHumanName',
      'dictionaryCitation'
    )

  return(gcam_data)
}

#' Retrives GDELT CAMEO religion code book
#'
#'
#' @return
#' @export
#' @importFrom magrittr %>%
#' @importFrom readr read_tsv
#' @examples
#' get_codes_cameo_religion
get_codes_cameo_religion <- function() {
  url <-
    'http://gdeltproject.org/data/lookups/CAMEO.religion.txt'
  code_df <-
    url %>%
    readr::read_tsv

  names(code_df) <-
    c('codeCAMEOReligion', 'nameCAMEOReligion')

  return(code_df)
}

#' Retrieves GDELT CAMEO country code book
#'
#'
#' @return
#' @importFrom magrittr %>%
#' @importFrom readr read_tsv
#' @export
#'
#' @examples
#' get_codes_cameo_country
get_codes_cameo_country <- function() {
  url <-
    'http://gdeltproject.org/data/lookups/CAMEO.country.txt'
  code_df <-
    url %>%
    read_tsv

  names(code_df) <-
    c('codeISO', 'nameCountry')

  return(code_df)
}

#' Retrieves GDELT CAMEO type code book
#'
#' @return
#' @importFrom magrittr %>%
#' @importFrom readr read_tsv
#' @export
#'
#' @examples
#' get_codes_cameo_type()
get_codes_cameo_type <- function() {
  url <- 'http://gdeltproject.org/data/lookups/CAMEO.type.txt'
  code_df <-
    url %>%
    read_tsv

  names(code_df) <-
    c('codeCAMEOType', 'nameCAMEOType')

  return(code_df)
}

#' Retrieves CAMEO CAMEO event code book
#'
#'
#' @return
#' @export
#'
#' @examples
#' get_codes_cameo_events()

get_codes_cameo_events <- function() {
  url <-
    'http://gdeltproject.org/data/lookups/CAMEO.eventcodes.txt'
  code_df <-
    url %>%
    read_tsv

  names(code_df) <-
    c('idCAMEOEvent', 'descriptionCAMEOEvent')

  code_df <-
    code_df %>%
    dplyr::mutate(isParentCode = ifelse(idCAMEOEvent %>% nchar == 2, T, F),
                  idParentCode = idCAMEOEvent %>% substr(1,2)) %>%
    dplyr::select(idParentCode, everything())

  return(code_df)
}

#' Retrieves GDELT CAMEO known group code book
#'
#' @return
#' @importFrom readr read_tsv
#' @importFrom magrittr %>%
#' @export
#' @examples
#' get_codes_cameo_known_groups()
get_codes_cameo_known_groups <- function() {
  url <-
    'http://gdeltproject.org/data/lookups/CAMEO.knowngroup.txt'
  code_df <-
    url %>%
    read_tsv

  names(code_df) <-
    c('codeCAMEOGroup', 'nameCAMEOGroup')

  return(code_df)
}

#' Retrieves GDELT CAMEO ethnic code book
#'
#' @return
#' @export
#'
#' @examples
#' get_codes_cameo_ethnic()
get_codes_cameo_ethnic <- function() {
  url <-
    'http://gdeltproject.org/data/lookups/CAMEO.ethnic.txt'
  code_df <-
    url %>%
    read_tsv

  names(code_df) <-
    c('codeCAMEOEthnicity', 'nameCAMEOEthnicity')

  return(code_df)
}

#' Retrieves GKG theme code book
#'
#' @return
#' @export
#'
#' @examples
#' get_codes_gkg_themes()
get_codes_gkg_themes <- function(split_word_bank_codes = F) {
  url <-
    'http://data.gdeltproject.org/documentation/GKG-MASTER-THEMELIST.TXT'

  code_df <-
    url %>%
    read_tsv(col_names = F)

  names(code_df) <-
    c('codeGKGTheme')
  code_df <-
    code_df %>%
    mutate(
      code2 = codeGKGTheme %>% str_to_lower(),
      isWBCode = ifelse(code2 %>% str_detect("wb_"), T, F),
      isEconomicEvent = ifelse(code2 %>% str_detect("econ_"), T, F),
      isSocialEvent = ifelse(code2 %>% str_detect("soc"), T, F),
      isTaxEvent = ifelse(code2 %>% str_detect("tax_"), T, F),
      isSocialEvent = ifelse(code2 %>% str_detect("soc_"), T, F),
      isMilitaryEvent = ifelse(code2 %>% str_detect("military|mil_"), T, F),
      isGovernmentEvent = ifelse(code2 %>% str_detect("gov_|government"), T, F),
      isMedicalEvent = ifelse(code2 %>% str_detect("med_|medical"), T, F),
      isAgressionAct = ifelse(code2 %>% str_detect("act_"), T, F),
      isMediaEvent = ifelse(code2 %>% str_detect("media_|_media"), T, F),
      isEmergencyEvent = ifelse(code2 %>% str_detect("emerg_"), T, F),
      isMovement = ifelse(code2 %>% str_detect("movement_"), T, F),
      isCriminalEvent = ifelse(code2 %>% str_detect("crime|crm_"), T, F)
    ) %>%
    dplyr::select(-code2)

  wb_codes <-
    code_df %>%
    dplyr::filter(isWBCode == T)

  wb_codes <-
    wb_codes %>%
    mutate(codeGKGTheme = codeGKGTheme %>% sub('\\_', '\\.', .)) %>%
    separate(
      codeGKGTheme,
      into = c('idDictionary', 'nameWBCode'),
      remove = F,
      sep = '\\.'
    ) %>%
    mutate(nameWBCode = nameWBCode %>% sub('\\_', '\\.', .)) %>%
    separate(
      nameWBCode,
      into = c('idWBCode', 'nameWBCode'),
      remove = T,
      sep = '\\.'
    ) %>%
    mutate(
      idWBCode = idWBCode %>% as.numeric,
      nameWBCode = nameWBCode %>% str_replace_all('\\_', ' ') %>% str_to_lower
    ) %>%
    dplyr::select(-idDictionary)

  non_wb <-
    code_df %>%
    dplyr::filter(isWBCode == F)

  code_df <-
    non_wb %>%
    bind_rows(wb_codes)

  return(code_df)

}

#' Retrieves GDELT event summary by period
#'
#' @param period can be \code{c("yearly", "daily", "monthly")}
#' @param by_country can be \code{c(TRUE, FALSE)}
#' @param return_message
#' @importFrom readr read_csv
#' @return
#' @export
#'
#' @examples
#' get_data_gdelt_period_event_totals(period = 'monthly', by_country = T)

get_data_gdelt_period_event_totals <- function(period = 'yearly',
                                               by_country = T,
                                               return_message = T) {
  periods <-
    c('daily', 'monthly', 'yearly')
  if (!period %in% periods) {
    "Sorry periods can only be:\n" %>%
      stop(paste0(paste0(periods, collapse = '\n')))
  }

  if (by_country == T) {
    period_slug <-
      period %>%
      paste0('_country.csv')
  } else {
    period_slug <-
      period %>%
      paste0('.csv')
  }
  base <-
    'http://data.gdeltproject.org/normfiles/'

  url_data <-
    base %>%
    paste0(period_slug)

  period_data <-
    url_data %>%
    read_csv(col_names = F) %>%
    suppressWarnings()

  if (by_country == T) {
    names(period_data) <-
      c('idDate', 'idCountry', 'countEvents')
  } else {
    names(period_data) <-
      c('idDate', 'countEvents')
  }

  period_data <-
    period_data %>%
    mutate(periodData = period, isByCountry = by_country)

  if (period == 'daily') {
    period_data <-
      period_data %>%
      mutate(dateData = idDate %>% ymd %>% as.Date()) %>%
      dplyr::select(periodData, isByCountry, dateData, everything())
  }

  if (period == 'monthly') {
    period_data <-
      period_data %>%
      mutate(year.month = idDate) %>%
      dplyr::select(periodData, isByCountry, year.month, everything())
  }

  if (period == 'yearly') {
    period_data <-
      period_data %>%
      mutate(yearData = idDate) %>%
      dplyr::select(periodData, isByCountry, everything())
  }

  if (return_message == T)  {
    from_date <-
      period_data$idDate %>% min

    to_date <-
      period_data$idDate %>% max

    total_events <-
      period_data$countEvents %>% sum() / 1000000
    events_slug <-
      total_events %>% paste0(" million GDELT events from ")
    "There have been " %>%
      paste0(events_slug, from_date,
             ' to ', to_date) %>%
      message
  }
  return(period_data)
}


#' Retrieves GDELT event database schema
#'
#' @return
#' @importFrom dplyr data_frame
#' @examples
#' get_schema_gdelt_events

get_schema_gdelt_events <- function() {
  gdelt_events_schema <-
    data_frame(
      nameGDELT = c(
        "GLOBALEVENTID",
        "SQLDATE",
        "MonthYear",
        "Year",
        "FractionDate",
        "Actor1Code",
        "Actor1Name",
        "Actor1CountryCode",
        "Actor1KnownGroupCode",
        "Actor1EthnicCode",
        "Actor1Religion1Code",
        "Actor1Religion2Code",
        "Actor1Type1Code",
        "Actor1Type2Code",
        "Actor1Type3Code",
        "Actor2Code",
        "Actor2Name",
        "Actor2CountryCode",
        "Actor2KnownGroupCode",
        "Actor2EthnicCode",
        "Actor2Religion1Code",
        "Actor2Religion2Code",
        "Actor2Type1Code",
        "Actor2Type2Code",
        "Actor2Type3Code",
        "IsRootEvent",
        "EventCode",
        "EventBaseCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "NumArticles",
        "AvgTone",
        "Actor1Geo_Type",
        "Actor1Geo_FullName",
        "Actor1Geo_CountryCode",
        "Actor1Geo_ADM1Code",
        "Actor1Geo_ADM2Code",
        "Actor1Geo_Lat",
        "Actor1Geo_Long",
        "Actor1Geo_FeatureID",
        "Actor2Geo_Type",
        "Actor2Geo_FullName",
        "Actor2Geo_CountryCode",
        "Actor2Geo_ADM1Code",
        "Actor2Geo_ADM2Code",
        "Actor2Geo_Lat",
        "Actor2Geo_Long",
        "Actor2Geo_FeatureID",
        "ActionGeo_Type",
        "ActionGeo_FullName",
        "ActionGeo_CountryCode",
        "ActionGeo_ADM1Code",
        "ActionGeo_ADM2Code",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        "ActionGeo_FeatureID",
        "DATEADDED",
        "SOURCEURL"
      ),
      nameActual = c(
        "idGlobalEvent",
        "dateEvent",
        "month_yearEvent",
        "yearEvent",
        "date_fractionEvent",
        "codeActor1",
        "nameActor1",
        "codeISOActor1",
        "codeCAMEOGroupActor1",
        "codeCAMEOEthnicityActor1",
        "codeCAMEOReligionActor1",
        "codeCAMEOReligion2Actor1",
        "codeCAMEOTypeActor1",
        "codeCAMEOType2Actor1",
        "codeCAMEOType3Actor1",
        "codeActor2",
        "nameActor2",
        "codeISOActor2",
        "codeCAMEOGroupActor2",
        "codeCAMEOEthnicityActor2",
        "codeCAMEOReligionActor2",
        "codeCAMEOReligion2Actor2",
        "codeCAMEOTypeActor2",
        "codeCAMEOType2Actor2",
        "codeCAMEOType3Actor.3",
        "isRootEvent",
        "codeEvent",
        "codeEventBase",
        "codeEventRoot",
        "classQuad",
        "scoreGoldstein",
        "countMentions",
        "countSources",
        "countArticles",
        "avgTone",
        "idTypeLocationActor1",
        "locationActor1",
        "idCountryActor1",
        "idADM1CodeActor1",
        "idADM2CodeActor1",
        "latitudeActor1",
        "longitudeActor1",
        "idFeatureActor1",
        "idTypeLocationActor2",
        "locationActor2",
        "idCountryActor2",
        "idADM1CodeActor2",
        "idADM2CodeActor2",
        "latitudeActor2",
        "longitudeActor2",
        "idFeatureActor2",
        "idTypeLocationAction",
        "locationAction",
        "idCountryAction",
        "idADM1CodeAction",
        "idADM2CodeAction",
        "latitudeAction",
        "longitudeAction",
        "idFeatureAction",
        "date_timeDataAdded",
        "urlSource"
      )
    )
  return(gdelt_events_schema)
}

#' Gets gkg general schema
#'
#' @return
#'
#' @examples
#' get_schema_gkg_general()
get_schema_gkg_general <- function() {
  schema_df <-
    data_frame(
      nameGDELT = c(
        "GKGRECORDID",
        "DATE",
        "SourceCollectionIdentifier",
        "SourceCommonName",
        "DocumentIdentifier",
        "Counts",
        "V2Counts",
        "Themes",
        "V2Themes",
        "Locations",
        "V2Locations",
        "Persons",
        "V2Persons",
        "Organizations",
        "V2Organizations",
        "V2Tone",
        "Dates",
        "GCAM",
        "SharingImage",
        "RelatedImages",
        "SocialImageEmbeds",
        "SocialVideoEmbeds",
        "Quotations",
        "AllNames",
        "Amounts",
        "TranslationInfo",
        "Extras",
        "NUMARTS",
        "COUNTS",
        "THEMES",
        "LOCATIONS",
        "PERSONS",
        "ORGANIZATIONS",
        "TONE",
        "CAMEOEVENTIDS",
        "SOURCES",
        "SOURCEURLS"

      ),
      nameActual = c(
        "idGKG",
        "dateURL",
        "isSourceCollectionIdentifier",
        "nameSource",
        "documentSource",
        "counts",
        "countsCharLoc",
        "themes",
        "themesCharLoc",
        "locations",
        "locationsCharLoc",
        "persons",
        "personsCharLoc",
        "organizations",
        "organizationsCharLoc",
        "tone",
        "dates",
        "gcam",
        "urlImage",
        "urlImageRelated",
        "urlSocialMediaImageEmbeds",
        "urlSocialMediaVideoEmbeds",
        "quotations",
        "mentionedNamesCounts",
        "mentionedNumericsCounts",
        "translationInfo",
        "xmlExtras",
        "countArticles",
        "counts",
        "themes",
        "locations",
        "persons",
        "organizations",
        "tone",
        "idCAMEOEvents",
        "sources",
        "urlSources"
      )
    )
  return(schema_df)
}


#' Gets gkg count schema
#'
#' @return
#'
#' @examples

get_schema_gkg_counts <- function() {
  counts_schema <-
    data_frame(
      nameGDELT = c(
        "DATE",
        "NUMARTS",
        "COUNTTYPE",
        "NUMBER",
        "OBJECTTYPE",
        "GEO_TYPE",
        "GEO_FULLNAME",
        "GEO_COUNTRYCODE",
        "GEO_ADM1CODE",
        "GEO_LAT",
        "GEO_LONG",
        "GEO_FEATUREID",
        "CAMEOEVENTIDS",
        "SOURCES",
        "SOURCEURLS"
      ),
      nameActual = c(
        "dateEvent",
        "countArticles",
        "typeEvent",
        "countObject",
        "typeObject",
        "idTypeLocation",
        "location",
        "idCountry",
        "idADM1CodeAction",
        "latitude",
        "longitude",
        "idFeature",
        "idCAMEOEvents",
        "sources",
        "urlSources"
      )
    )
  return(counts_schema)
}



#' Gets gkg mention schema
#'
#' @return
#'
#' @examples
#' get_schema_gkg_mentions()
get_schema_gkg_mentions <- function() {
  mentions_schema <-
    data_frame(
      nameGDELT =
        c(
          "GLOBALEVENTID",
          "EventTimeDate",
          "MentionTimeDate",
          "MentionType",
          "MentionSourceName",
          "MentionIdentifier",
          "SentenceID",
          "Actor1CharOffset",
          "Actor2CharOffset",
          "ActionCharOffset",
          "InRawText",
          "Confidence",
          "MentionDocLen",
          "MentionDocTone",
          "MentionDocTranslationInfo",
          "Extras"
        ),
      nameActual =
        c(
          "idGlobalEvent",
          "dateEvent",
          "dateMention",
          "idMentionType",
          "nameSource",
          "documentSource",
          "idSentence",
          "charLocActor1",
          "charLocActor2",
          "charLocAction",
          "isRawText",
          "scoreGoldsteinConfidence",
          "lengthMentionedDocument",
          "toneMentionedDocument",
          "translationMentionedDocument",
          "extra"
        )
    )

  return(mentions_schema)
}

#' Retrieves GDELT Event or GKG data from a given URL
#'
#' @param url
#' @param file_directory
#' @param remove_files
#' @param empty_trash
#' @param return_message
#' @importFrom purrr flatten_chr
#' @importFrom tidyr extract_numeric
#' @importFrom purrr compact
#' @import dplyr
#' @import utils
#' @importFrom urltools domain
#' @importFrom curl curl_download
#' @return
#' @export
#'
#' @examples
#' get_gdelt_url_data(url = "http://data.gdeltproject.org/gdeltv2/20160531000000.gkg.csv.zip", file_directory = 'Desktop/temp_gdelt_data', remove_files = T, empty_trash = T, return_message = T)
get_gdelt_url_data <-
  function(url = "http://data.gdeltproject.org/gdeltv2/20160531000000.gkg.csv.zip",
           file_directory = 'Desktop/temp_gdelt_data',
           remove_files = T,
           empty_trash = T,
           return_message = T) {

    files <-
      url %>%
      str_replace_all(
        'http://data.gdeltproject.org/gdeltv2/|http://data.gdeltproject.org/gkg/|http://data.gdeltproject.org/events/|http://data.gdeltproject.org/gdeltv2_cloudvision/',
        ''
      ) %>%
      str_split('\\.') %>%
      flatten_chr

    file_name <-
      files %>%
      .[1] %>%
      paste0(".zip")

    file_type <-
      files %>%
      .[2]

    temp.dir <-
      file_directory

    file_path <-
      temp.dir %>% str_split('/') %>% flatten_chr() %>% .[1:length(.)] %>% paste0(collapse = '/')

    if (!dir.exists(paths = file_path)) {
      dir.create(temp.dir)
    }

    file <-
      temp.dir %>%
      paste0('/', file_name)

    url %>%
      curl_download(url = ., destfile = file)

    file %>%
      unzip(exdir = paste0(temp.dir, '/'))

    dir_files <-
      temp.dir %>%
      list.files()

    csv_file_loc <-
      dir_files[dir_files %>%
                  str_detect(".csv|.CSV")] %>%
      paste0(temp.dir, '/', .)

    gdelt_cols <-
      csv_file_loc %>%
      read_tsv(col_names = F,
               n_max = 1) %>% ncol %>% suppressWarnings() %>% extract_numeric()


    if (gdelt_cols == 16) {
      gdelt_data <-
        csv_file_loc %>%
        read_tsv(col_names = F) %>%
        suppressWarnings()

      names(gdelt_data) <-
        get_schema_gkg_mentions() %>% .$nameActual

      gdelt_data <-
        gdelt_data %>%
        mutate(
          date_timeEvent = dateEvent %>% ymd_hms %>% with_tz(Sys.timezone()),
          dateEvent = date_timeEvent %>% as.Date(),
          date_timeMention = dateMention %>% ymd_hms %>% with_tz(Sys.timezone()),
          dateMention = date_timeMention %>% as.Date()
        ) %>%
        dplyr::select(idGlobalEvent,
                      date_timeEvent,
                      date_timeMention,
                      everything()) %>%
        dplyr::left_join(data_frame(
          idMentionType = 1:6,
          mention_type = c('web', 'citation', 'core', 'dtic', 'jstor', 'nontext')
        )) %>%
        dplyr::select(idGlobalEvent:idMentionType,
                      mention_type,
                      everything()) %>%
        suppressMessages()

      gdelt_data <-
        gdelt_data %>%
        mutate_each_(funs(as.logical(.)),
                     gdelt_data %>% dplyr::select(matches("is")) %>% names)

    }

    if (gdelt_cols == 15) {
      gdelt_data <-
        csv_file_loc %>%
        read_tsv(col_names = T) %>%
        suppressWarnings()

      names(gdelt_data) <-
        get_schema_gkg_counts() %>% .$nameActual

      gdelt_data <-
        gdelt_data %>%
        dplyr::left_join(data_frame(
          idTypeLocation = 1:5,
          typeLocation = c(
            'country',
            'usState',
            'usCity',
            'wordCity',
            'worldState'
          )
        )) %>%
        suppressMessages() %>%
        dplyr::mutate(
          idRecord = 1:n(),
          idGKG = dateEvent %>% paste0('.', idRecord),
          urlSources = urlSources %>% str_replace_all("<UDIV>", ';')
        ) %>%
        dplyr::mutate(dateEvent = dateEvent %>% ymd()) %>%
        dplyr::select(dateEvent:idTypeLocation, typeLocation, everything()) %>%
        dplyr::select(idRecord, idGKG, everything())
    }

    if (gdelt_cols == 61) {
      gdelt_data <-
        csv_file_loc %>%
        readr::read_tsv(col_names = F) %>%
        suppressWarnings()

      names(gdelt_data) <-
        get_schema_gdelt_events() %>% .$nameActual

      gdelt_data <-
        gdelt_data %>%
        dplyr::rename(date_timeURL = date_timeDataAdded) %>%
        dplyr::mutate(
          dateEvent = dateEvent %>% lubridate::ymd,
          date_timeURL = date_timeURL %>% ymd_hms() %>% with_tz(Sys.timezone()),
          nameSource = urlSource %>% domain() %>% str_replace_all("www.", '')
        )

      gdelt_data <-
        gdelt_data %>%
        dplyr::mutate_each_(funs(as.logical(.)),
                            gdelt_data %>% dplyr::select(matches("is")) %>% names)

      gdelt_data <-
        gdelt_data %>%
        dplyr::left_join(data_frame(
          classQuad =  1:4,
          nameQuad =  c(
            'Verbal Cooperation',
            'Material Cooperation',
            'Verbal Conflict',
            'Material Conflict'
          )
        )) %>%
        suppressMessages()
    }

    if (gdelt_cols == 57) {
      gdelt_data <-
        csv_file_loc %>%
        readr::read_tsv(col_names = F) %>%
        suppressWarnings()

      names(gdelt_data) <-
        c(
          "idGlobalEvent",
          "dateEvent",
          "month_yearEvent",
          "yearEvent",
          "date_fractionEvent",
          "codeActor1",
          "nameActor1",
          "codeISOActor1",
          "codeCAMEOGroupActor1",
          "codeCAMEOEthnicityActor1",
          "codeCAMEOReligionActor1",
          "codeCAMEOReligion2Actor1",
          "codeCAMEOTypeActor1",
          "codeCAMEOType2Actor1",
          "codeCAMEOType3Actor1",
          "codeActor2",
          "nameActor2",
          "codeISOActor2",
          "codeCAMEOGroupActor2",
          "codeCAMEOEthnicityActor2",
          "codeCAMEOReligionActor2",
          "codeCAMEOReligion2Actor2",
          "codeCAMEOTypeActor2",
          "codeCAMEOType2Actor2",
          "codeCAMEOType3Actor.3",
          "isRootEvent",
          "codeEvent",
          "codeEventBase",
          "codeEventRoot",
          "classQuad",
          "scoreGoldstein",
          "countMentions",
          "countSources",
          "countArticles",
          "avgTone",
          "idTypeLocationActor1",
          "locationActor1",
          "idCountryActor1",
          "idADM1CodeActor1",
          "latitudeActor1",
          "longitudeActor1",
          "idFeatureActor1",
          "idTypeLocationActor2",
          "locationActor2",
          "idCountryActor2",
          "idADM1CodeActor2",
          "latitudeActor2",
          "longitudeActor2",
          "idFeatureActor2",
          "idTypeLocationAction",
          "locationAction",
          "idCountryAction",
          "idADM1CodeAction",
          "latitudeAction",
          "longitudeAction",
          "idFeatureAction",
          "dateAdded"
        )

      gdelt_data <-
        gdelt_data %>%
        dplyr::rename(dateURL = dateAdded) %>%
        dplyr::mutate(
          dateEvent = dateEvent %>% lubridate::ymd,
          dateURL = dateURL %>% lubridate::ymd
        ) %>%
        suppressWarnings()

      gdelt_data <-
        gdelt_data %>%
        dplyr::mutate_each_(funs(as.logical(.)),
                            gdelt_data %>% dplyr::select(matches("is")) %>% names)

      gdelt_data <-
        gdelt_data %>%
        dplyr::left_join(data_frame(
          classQuad =  1:4,
          nameQuad =  c(
            'Verbal Cooperation',
            'Material Cooperation',
            'Verbal Conflict',
            'Material Conflict'
          )
        )) %>%
        suppressMessages()
    }

    if (gdelt_cols == 58) {
      load_needed_packages(c('urltools'))
      gdelt_data <-
        csv_file_loc %>%
        readr::read_tsv(col_names = F) %>%
        suppressWarnings()
      names(gdelt_data) <-
        c(
          "idGlobalEvent",
          "dateEvent",
          "month_yearEvent",
          "yearEvent",
          "date_fractionEvent",
          "codeActor1",
          "nameActor1",
          "codeISOActor1",
          "codeCAMEOGroupActor1",
          "codeCAMEOEthnicityActor1",
          "codeCAMEOReligionActor1",
          "codeCAMEOReligion2Actor1",
          "codeCAMEOTypeActor1",
          "codeCAMEOType2Actor1",
          "codeCAMEOType3Actor1",
          "codeActor2",
          "nameActor2",
          "codeISOActor2",
          "codeCAMEOGroupActor2",
          "codeCAMEOEthnicityActor2",
          "codeCAMEOReligionActor2",
          "codeCAMEOReligion2Actor2",
          "codeCAMEOTypeActor2",
          "codeCAMEOType2Actor2",
          "codeCAMEOType3Actor.3",
          "isRootEvent",
          "codeEvent",
          "codeEventBase",
          "codeEventRoot",
          "classQuad",
          "scoreGoldstein",
          "countMentions",
          "countSources",
          "countArticles",
          "avgTone",
          "idTypeLocationActor1",
          "locationActor1",
          "idCountryActor1",
          "idADM1CodeActor1",
          "latitudeActor1",
          "longitudeActor1",
          "idFeatureActor1",
          "idTypeLocationActor2",
          "locationActor2",
          "idCountryActor2",
          "idADM1CodeActor2",
          "latitudeActor2",
          "longitudeActor2",
          "idFeatureActor2",
          "idTypeLocationAction",
          "locationAction",
          "idCountryAction",
          "idADM1CodeAction",
          "latitudeAction",
          "longitudeAction",
          "idFeatureAction",
          "dateAdded",
          "urlSource"
        )

      gdelt_data <-
        gdelt_data %>%
        dplyr::rename(date_timeURL = dateAdded) %>%
        dplyr::mutate(
          dateEvent = dateEvent %>% lubridate::ymd,
          date_timeURL %>% lubridate::ymd_hms() %>% with_tz(Sys.timezone()),
          dateURL = date_timeURL %>% as.Date(),
          nameSource = urlSource %>% domain() %>% str_replace_all("www.", '')
        ) %>%
        suppressWarnings()

      gdelt_data <-
        gdelt_data %>%
        dplyr::mutate_each_(funs(as.logical(.)),
                            gdelt_data %>% dplyr::select(matches("is")) %>% names)

      gdelt_data <-
        gdelt_data %>%
        dplyr::left_join(data_frame(
          classQuad =  1:4,
          nameQuad =  c(
            'Verbal Cooperation',
            'Material Cooperation',
            'Verbal Conflict',
            'Material Conflict.'
          )
        )) %>%
        suppressMessages()

    }

    if (gdelt_cols == 11) {
      gdelt_data <-
        csv_file_loc %>%
        readr::read_tsv(col_names = T) %>%
        suppressWarnings()

      schema_df <-
        get_schema_gkg_general()

      names(gdelt_data) <-
        schema_df$nameActual[names(gdelt_data) %>% match(schema_df$nameGDELT)]

      names(gdelt_data)[1] <-
        c('date')

      gdelt_data <-
        gdelt_data %>%
        dplyr::mutate(
          idRecord = 1:n(),
          idGKG = date %>% paste0('.', idRecord),
          date = date %>% lubridate::ymd(),
          urlSources = urlSources %>% str_replace_all("<UDIV>", ';')
        ) %>%
        dplyr::select(idRecord, idGKG, everything())

    }

    if (gdelt_cols == 27) {
      gdelt_data <-
        csv_file_loc %>%
        readr::read_tsv(col_names = F) %>%
        suppressWarnings()

      schema_df <-
        get_schema_gkg_general()

      names(gdelt_data) <-
        schema_df$nameActual[1:27]

      gdelt_data <-
        gdelt_data %>%
        dplyr::mutate(
          isSourceCollectionIdentifier = isSourceCollectionIdentifier %>% as.numeric(),
          isDocumentURL = ifelse(documentSource %>% str_detect('http'), T, F)
        ) %>%
        dplyr::select(idGKG:isSourceCollectionIdentifier,
                      isDocumentURL,
                      everything()) %>%
        dplyr::rename(date_timeURL = dateURL) %>%
        dplyr::mutate(date_timeURL = date_timeURL %>% lubridate::ymd_hms() %>% with_tz(Sys.timezone())) %>%
        separate(
          idGKG,
          into = c('date_time', 'idDate_timeArticle'),
          sep = '\\-',
          remove = F
        ) %>%
        dplyr::select(-date_time) %>%
        dplyr::mutate(idDate_timeArticle = idDate_timeArticle %>% as.numeric) %>%
        suppressMessages() %>%
        suppressWarnings()

    }

    if (remove_files == T) {
      "rm -R " %>%
        paste0(temp.dir) %>%
        system()
      if (empty_trash == T) {
        system('rm -rf ~/.Trash/*')
      }
    }


    if ('idADM1CodeActor1' %in% names(gdelt_data)) {
      gdelt_data <-
        gdelt_data %>%
        mutate(idADM1CodeActor1 = idADM1CodeActor1 %>% as.character())

    }

    if ('idADM1CodeActor2' %in% names(gdelt_data)) {
      gdelt_data <-
        gdelt_data %>%
        mutate(idADM1CodeActor2 = idADM1CodeActor2 %>% as.character())

    }

    if ('idADM2CodeActor1' %in% names(gdelt_data)) {
      gdelt_data <-
        gdelt_data %>%
        mutate(idADM2CodeActor1 = idADM2CodeActor1 %>% as.character())

    }

    if ('idADM2CodeActor2' %in% names(gdelt_data)) {
      gdelt_data <-
        gdelt_data %>%
        mutate(idADM2CodeActor2 = idADM2CodeActor2 %>% as.character())

    }

    if ('idADM1CodeAction' %in% names(gdelt_data)) {
      gdelt_data <-
        gdelt_data %>%
        mutate(idADM1CodeAction = idADM1CodeAction %>% as.character())

    }

    if ('idADM2CodeAction' %in% names(gdelt_data)) {
      gdelt_data <-
        gdelt_data %>%
        mutate(idADM2CodeAction = idADM2CodeAction %>% as.character())

    }

    if (return_message) {
      "Downloaded, parsed and imported " %>%
        paste0(url) %>%
        message()

    }

    return(gdelt_data)

  }

#' Returns long or wide mentioned numerics from a GKG data frame
#'
#' @param gdelt_data
#' @param filter_na
#' @param include_char_locg
#' @param return_wide
#' @importFrom tidyr gather
#' @importFrom tidyr unite
#' @importFrom tidyr spread
#' @importFrom tidyr separate
#' @importFrom purrr map
#' @importFrom purrr compact
#' @import stringr
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_numerics <- function(gdelt_data,
                                       filter_na = T,
                                       include_char_loc = T,
                                       return_wide = F) {
  parse_mentioned_numerics <-
    function(field = "170,Scotland Road,1600;170,Scotland Road,2475;",
             return_wide = F) {
      options(scipen = 99999)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(
              amountValue1 = NA,
              amountTerm1 = NA,
              charLoc = NA
            )
        } else {
          field_data <-
            data_frame(
              amountValue = NA,
              amountTerm = NA,
              charLoc = NA,
              idArticleNumericItem = 1
            )
        }
      } else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(field = fields) %>%
          dplyr::mutate(idArticleNumericItem = 1:n()) %>%
          separate(
            field,
            into = c('amountValue', 'amountTerm', 'charLoc'),
            sep = '\\,'
          ) %>%
          dplyr::mutate(amountTerm = amountTerm %>% str_trim) %>%
          suppressMessages() %>%
          suppressWarnings()

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticleNumericItem) %>%
            arrange(idArticleNumericItem) %>%
            unite(item, item, idArticleNumericItem, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(extract_numeric(.)),
                                vars =
                                  field_data %>% dplyr::select(matches('amountValue|charLoc')) %>% names)
        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticleNumericItem,
                          amountValue,
                          amountTerm,
                          charLoc) %>%
            dplyr::mutate(charLoc = charLoc %>% as.numeric,
                          amountValue = amountValue %>% as.numeric)
        }
      }

      return(field_data)
    }

  if (!'mentionedNumericsCounts' %in% names(gdelt_data)) {
    stop("Sorry missing numeric count column")
  }
  counts_data <-
    gdelt_data %>%
    dplyr::select(idGKG, mentionedNumericsCounts)

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!mentionedNumericsCounts %>% is.na)
  }

  all_counts <-
    1:length(counts_data$mentionedNumericsCounts) %>%
    purrr::map(function(x) {
      parse_mentioned_numerics(field = counts_data$mentionedNumericsCounts[x],
                               return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  if (include_char_loc == F) {
    all_counts <-
      all_counts %>%
      dplyr::select(-charLoc)
  }

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  return(all_counts)
}

#' Returns long or wide mentioned people from a GKG data frame
#'
#' @param gdelt_data
#' @param people_column options \code{c('person', 'persons', 'persons.count', 'personsCharLoc', 'charLoc'))}
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_people <- function(gdelt_data,
                                     people_column = 'persons',
                                     filter_na = T,
                                     return_wide = F) {

  people_count_cols <-
    c('person',
      'persons',
      'persons.count',
      'personsCharLoc',
      'charLoc')

  if (!people_column %in% people_count_cols) {
    stop("Sorry people column can only be\n" %>%
           paste0(paste0(people_count_cols, collapse = '\n')))
  }

  if (people_column %in% c('person', 'persons')) {
    people_column <-
      'persons'
  }

  if (people_column %in% c('persons.count', 'personsCharLoc', 'charLoc')) {
    people_column <-
      'personsCharLoc'
  }

  parse_mentioned_people_counts <-
    function(field = "Chaudhry Nisar Ali Khan,63",
             return_wide = F) {
      options(scipen = 99999)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(namePerson1 = NA, charLoc1 = NA)
        } else {
          field_data <-
            data_frame(
              namePerson = NA,
              charLoc = NA,
              idArticlePerson = 1
            )
        }
      } else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(field = fields) %>%
          dplyr::mutate(idArticlePerson = 1:n()) %>%
          separate(field,
                   into = c('namePerson', 'charLoc'),
                   sep = '\\,') %>%
          suppressWarnings() %>%
          suppressMessages()

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticlePerson) %>%
            arrange(idArticlePerson) %>%
            unite(item, item, idArticlePerson, sep = '.') %>%
            suppressWarnings()

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(extract_numeric(.)),
                                vars =
                                  field_data %>% dplyr::select(matches('charLoc')) %>% names)
        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticlePerson, namePerson, charLoc) %>%
            dplyr::mutate(charLoc = charLoc %>% as.numeric)
        }
      }

      return(field_data)
    }

  if (!people_column %in% names(gdelt_data)) {
    stop("Sorry missing people column")
  }

  col_names <-
    c('idGKG', people_column)

  counts_data <-
    gdelt_data %>%
    dplyr::select_(.dots = col_names)

  names(counts_data)[2] <-
    'people_col'

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!people_col %>% is.na())
  }

  all_counts <-
    1:length(counts_data$people_col) %>%
    purrr::map(function(x) {
      parse_mentioned_people_counts(field = counts_data$people_col[x],
                                    return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  if (people_column == 'persons') {
    all_counts <-
      all_counts %>%
      dplyr::select(-charLoc)
  }

  return(all_counts)
}

#' Returns long or wide mentioned organizations from a GKG data frame
#'
#' @param gdelt_data
#' @param organization_column options \code{c('organization', 'organizations', 'organizations.count', 'organizationsCharLoc', 'charLoc'))}
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_organizations <- function(gdelt_data,
                                            organization_column = 'organizations',
                                            filter_na = T,
                                            return_wide = F) {

  organization_count_cols <-
    c(
      'organization',
      'organizations',
      'organizations.count',
      'organizationsCharLoc',
      'charLoc'
    )

  if (!organization_column %in% organization_count_cols) {
    stop("Sorry people column can only be\n" %>%
           paste0(paste0(organization_count_cols, collapse = '\n')))
  }

  if (organization_column %in% c('organization', 'organizations')) {
    organization_column <-
      'organizations'
  }

  if (organization_column %in% c('organizations.count',
                                 'organizationsCharLoc',
                                 'charLoc')) {
    organization_column <-
      'personsCharLoc'
  }

  parse_mentioned_organization_counts <-
    function(field = "Twitter,2912;Pegasystems,169;Pegasystems,1238;Pegasystems,1829;Pegasystems,2079;Pegasystems,2888;Nasdaq,193;Nasdaq,2086;Ilena Ryan Pegasystems Inc,2892;Pegasystems Inc,173;Pegasystems Inc,2892",
             return_wide = F) {
      options(scipen = 99999)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(nameOrganization1 = NA,
                       charLoc1 = NA)
        } else {
          field_data <-
            data_frame(
              nameOrganization = NA,
              charLoc = NA,
              idArticle.organization = 1
            )
        }
      } else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(field = fields) %>%
          dplyr::mutate(idArticle.organization = 1:n()) %>%
          separate(field,
                   into = c('nameOrganization', 'charLoc'),
                   sep = '\\,') %>%
          suppressMessages() %>%
          suppressWarnings()

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticle.organization) %>%
            arrange(idArticle.organization) %>%
            unite(item, item, idArticle.organization, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(extract_numeric(.)),
                                vars =
                                  field_data %>% dplyr::select(matches('charLoc')) %>% names)
        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticle.organization, nameOrganization, charLoc) %>%
            dplyr::mutate(charLoc = charLoc %>% as.numeric)
        }
      }

      return(field_data)
    }

  if (!organization_column %in% names(gdelt_data)) {
    stop("Sorry missing organization column")
  }

  col_names <-
    c('idGKG', organization_column)

  counts_data <-
    gdelt_data %>%
    dplyr::select_(.dots = col_names)

  names(counts_data)[2] <-
    'org_col'

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!org_col %>% is.na())
  }

  all_counts <-
    1:length(counts_data$org_col) %>%
    purrr::map(function(x) {
      parse_mentioned_organization_counts(field = counts_data$org_col[x],
                                          return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  if (organization_column == 'organizations') {
    all_counts <-
      all_counts %>%
      dplyr::select(-charLoc)
  }

  return(all_counts)
}

#' Returns mentioned names from a GKG data frame.
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_names <- function(gdelt_data,
                                    filter_na = T,
                                    return_wide = F) {
  parse_mentioned_names_counts <-
    function(field = "Interior Minister Chaudhry Nisar Ali Khan,47;Mullah Mansour,87;Afghan Taliban,180;Mullah Mansour,382;Mullah Mansor,753;Mullah Mansour,815;Mullah Mansour,1025",
             return_wide = F) {
      options(scipen = 99999)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(nameMentionedName1 = NA,
                       charLoc1 = NA)
        } else {
          field_data <-
            data_frame(
              nameMentionedName = NA,
              charLoc = NA,
              idArticleMentionedName = 1
            )
        }
      } else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(field = fields) %>%
          dplyr::mutate(idArticleMentionedName = 1:n()) %>%
          separate(field,
                   into = c('nameMentionedName', 'charLoc'),
                   sep = '\\,') %>%
          suppressMessages() %>%
          suppressWarnings()

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticleMentionedName) %>%
            arrange(idArticleMentionedName) %>%
            unite(item, item, idArticleMentionedName, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(extract_numeric(.)),
                                vars =
                                  field_data %>% dplyr::select(matches('charLoc')) %>% names)
        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticleMentionedName,
                          nameMentionedName,
                          charLoc) %>%
            dplyr::mutate(charLoc = charLoc %>% as.numeric)
        }
      }

      return(field_data)
    }

  if (!'mentionedNamesCounts' %in% names(gdelt_data)) {
    stop("Sorry missing metioned name column")
  }
  counts_data <-
    gdelt_data %>%
    dplyr::select(idGKG, mentionedNamesCounts)

  all_counts <-
    1:length(counts_data$mentionedNamesCounts) %>%
    purrr::map(function(x) {
      parse_mentioned_names_counts(field = counts_data$mentionedNamesCounts[x],
                                   return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())
  if (filter_na == T) {
    if ('nameMentionedName' %in% names(all_counts)) {
      all_counts <-
        all_counts %>%
        dplyr::filter(!nameMentionedName %>% is.na)
    }
    if ('nameMentionedName1' %in% names(all_counts)) {
      all_counts <-
        all_counts %>%
        dplyr::filter(!nameMentionedName1 %>% is.na)
    }
  }
  return(all_counts)
}

#' Returns mentioned themes from a gkg data frame
#'
#' @param gdelt_data
#' @param filter_na
#' @param organization_column options \code{c('theme', 'themes', 'countThemes', 'themesCharLoc', 'charLoc'))}
#' @param return_wide
#' @importFrom purrr map
#'
#' @return
#' @export
#'
#' @examples

get_mentioned_gkg_themes <- function(gdelt_data,
                                     filter_na = T,
                                     theme_column = 'themes',
                                     return_wide = F) {
  theme_count_cols <-
    c('theme',
      'themes',
      'countThemes',
      'themesCharLoc',
      'charLoc')

  if (!theme_column %in% theme_count_cols) {
    stop("Sorry theme column can only be\n" %>%
           paste0(paste0(theme_count_cols, collapse = '\n')))
  }

  if (theme_column %in% c('theme', 'themes')) {
    theme_column <-
      'themes'
  }

  if (theme_column %in% c('countThemes', 'themesCharLoc', 'charLoc')) {
    theme_column <-
      'themesCharLoc'
  }

  parse_mentioned_names_themes <-
    function(field = "https://youtube.com/esctodaytv;https://youtube.com/embed/5ymFX91HwM0?wmode=transparent&#038;modestbranding=1&#038;autohide=1&#038;showinfo=0&#038;rel=0;",
             return_wide = F) {
      options(scipen = 99999)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(codeTheme1 = NA, charLoc1 = NA)
        } else {
          field_data <-
            data_frame(
              codeTheme = NA,
              charLoc = NA,
              idArticleGKGTheme = 1
            )
        }
      } else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(field = fields) %>%
          dplyr::mutate(idArticleGKGTheme = 1:n()) %>%
          separate(field,
                   into = c('codeTheme', 'charLoc'),
                   sep = '\\,') %>%
          suppressWarnings()

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticleGKGTheme) %>%
            arrange(idArticleGKGTheme) %>%
            unite(item, item, idArticleGKGTheme, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(extract_numeric(.)),
                                vars =
                                  field_data %>% dplyr::select(matches('charLoc')) %>% names)
        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticleGKGTheme, codeTheme, charLoc) %>%
            dplyr::mutate(charLoc = charLoc %>% as.numeric)
        }
      }

      return(field_data)
    }

  if (!theme_column %in% names(gdelt_data)) {
    stop("Sorry missing organization column")
  }

  col_names <-
    c('idGKG', theme_column)

  counts_data <-
    gdelt_data %>%
    dplyr::select_(.dots = col_names)
  names(counts_data)[2] <-
    'theme_col'

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!theme_col %>% is.na())
  }

  all_counts <-
    1:length(counts_data$theme_col) %>%
    purrr::map(function(x) {
      parse_mentioned_names_themes(field = counts_data$theme_col[x],
                                   return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  if (theme_column == 'themes') {
    all_counts <-
      all_counts %>%
      dplyr::select(-charLoc)
  }
  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  return(all_counts)
}

#' Returns social embed information from a gkg data frame
#'
#' @param gdelt_data
#' @param organization_column options \code{c('urlSocialMediaImageEmbeds', 'images', 'urlSocialMediaVideoEmbeds', 'video', 'videos'))}
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples

get_mentioned_gkg_social_embeds <- function(gdelt_data,
                                            social_embed_column = 'urlSocialMediaImageEmbeds',
                                            filter_na = T,
                                            return_wide = F) {
  image_video_cols <-
    c(
      'urlSocialMediaImageEmbeds',
      'images',
      'image',
      'urlSocialMediaVideoEmbeds',
      'video',
      'videos'
    )
  if (!social_embed_column %in% image_video_cols) {
    stop("Social embed column can only be\n" %>% paste0(paste0(image_video_cols, collapse = '\n')))
  }

  if (social_embed_column %in% c("image", "images")) {
    social_embed_column <-
      'urlSocialMediaImageEmbeds'
  }

  if (social_embed_column  %in% c("video", "videos")) {
    social_embed_column <-
      'urlSocialMediaVideoEmbeds'
  }
  parse_embeds <-
    function(field = "http://instagram.com/p/9YfHJtMx0N;http://instagram.com/p/BFz1t7Tsx8t;http://instagram.com/p/BEZrBSKsx8U;http://instagram.com/p/BEw5_T-Mx3B;",
             return_wide = F) {
      options(scipen = 99999)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(urlSocialMediaImageEmbed = NA)
        } else {
          field_data <-
            data_frame(
              urlSocialMediaImageEmbed = NA,
              idArticleSocialMediaImageEmbed = 1
            )
        }
      }  else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(urlSocialMediaImageEmbed = fields) %>%
          dplyr::mutate(
            idArticleSocialMediaImageEmbed = 1:n(),
            domainSocialMediaImageEmbed = urlSocialMediaImageEmbed %>% urltools::domain()
          )
        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticleSocialMediaImageEmbed) %>%
            arrange(idArticleSocialMediaImageEmbed) %>%
            unite(item, item, idArticleSocialMediaImageEmbed, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(
              idArticleSocialMediaImageEmbed,
              domainSocialMediaImageEmbed,
              urlSocialMediaImageEmbed
            )
        }
      }

      return(field_data)
    }


  if (!social_embed_column %in% names(gdelt_data)) {
    stop("Sorry missing source embed column")
  }

  col_names <-
    c('idGKG', social_embed_column)

  counts_data <-
    gdelt_data %>%
    dplyr::select_(.dots = col_names)

  names(counts_data)[2] <-
    'source_col'

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!source_col %>% is.na())
  }

  all_counts <-
    1:length(counts_data$source_col) %>%
    purrr::map(function(x) {
      parse_embeds(field = counts_data$source_col[x],
                   return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  if (social_embed_column == 'urlSocialMediaImageEmbeds') {
    names(all_counts)[3] <-
      c('urlSocialMediaImage.embed')

    names(all_counts)[1] <-
      c('idArticleSocialMediaImageEmbed')
  }

  if (social_embed_column == 'urlSocialMediaVideoEmbeds') {
    names(all_counts)[3] <-
      c('urlSocialMediaVideo.embed')
    names(all_counts)[1] <-
      c('idArticle.social_media.video_embed')
  }

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  return(all_counts)
}

#' Returns article tones from a gkg data frame
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_article_tone <- function(gdelt_data,
                                           filter_na = T,
                                           return_wide = F) {
  parse_article_tones <-
    function(field = "-4.65116279069767,1.55038759689922,62015503875969,7.75193798449612,13.1782945736434,0,134",
             return_wide = F) {
      options(scipen = 99999, digits = 5)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(amount.tone = NA)
        } else {
          field_data <-
            data_frame(amount.tone = NA, idArticle.tone = 1)
        }
      }  else {
        fields <-
          field %>%
          str_split('\\,') %>%
          flatten_chr() %>%
          .[!. %in% ''] %>%
          as.numeric()

        fields_df <-
          data_frame(amount.tone = fields) %>%
          dplyr::mutate(idArticle.tone = 1:n())
        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticle.tone) %>%
            arrange(idArticle.tone) %>%
            unite(item, item, idArticle.tone, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticle.tone, amount.tone)
        }
      }

      return(field_data)
    }

  if (!'tone' %in% names(gdelt_data)) {
    stop("Sorry missing tone column")
  }
  counts_data <-
    gdelt_data %>%
    dplyr::select(idGKG, tone)

  all_counts <-
    1:length(counts_data$tone) %>%
    purrr::map(function(x) {
      parse_article_tones(field = counts_data$tone[x], return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())
  if (filter_na == T) {
    all_counts <-
      all_counts %>%
      dplyr::filter(!amount.tone %>% is.na)
  }
  return(all_counts)
}

#' Returns mentioned CAMEO event count from a gkg data frame
#'
#' @param gdelt_data
#' @param count_column options \code{c('count', 'counts'))}
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_event_counts <- function(gdelt_data,
                                           count_column = 'counts',
                                           filter_na = T,
                                           return_wide = F) {
  count_cols <-
    c('counts',
      'count',
      'countsCharLoc',
      'countCharLoc',
      'charLoc')

  if (!count_column %in% count_cols) {
    stop("Sorry count column can only be\n" %>%
           paste0(paste0(count_cols, collapse = '\n')))
  }

  if (count_column %in% c('counts', 'count')) {
    count_column <-
      'counts'
  }

  if (count_column %in% c('countsCharLoc', 'countCharLoc', 'charLoc')) {
    count_column <-
      'countsCharLoc'
  }

  parse_field_count <-
    function(field = "KIDNAP#60##4#Beirut, Beyrouth, Lebanon#LE#LE04#33.8719#35.5097#-801546;KIDNAP#2##1#Lebanon#LE#LE#33.8333#35.8333#LE;KIDNAP#4##1#Australia#AS#AS#-27#133#AS;",
             return_wide = F) {
      options(scipen = 99999, digits = 5)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(codeGKGTheme = NA)
        } else {
          field_data <-
            data_frame(codeGKGTheme = NA,
                       idArticle.field = 1)
        }
      }  else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(field_item = fields) %>%
          dplyr::mutate(idArticle.field = 1:n()) %>%
          separate(
            col = field_item,
            sep = '\\#',
            into = c(
              'codeGKGTheme',
              'countEvent',
              'entityEvent',
              'idTypeLocation',
              'location',
              'idCountry',
              'idADM1Code',
              'latitude',
              'longitude',
              'idFeature',
              'charLoc'
            )
          ) %>%
          suppressMessages() %>%
          suppressWarnings()

        fields_df <-
          fields_df %>%
          dplyr::mutate_each_(funs(as.numeric),
                              vars =
                                fields_df %>% dplyr::select(matches("count|charLoc|idTypeLocation")) %>% dplyr::select(-idCountry) %>% names) %>%
          dplyr::mutate_each_(funs(as.numeric(., digits = 5)),
                              vars =
                                fields_df %>% dplyr::select(matches("latitude|longitude")) %>% names)

        fields_df$entityEvent[fields_df$entityEvent == ''] <-
          NA

        fields_df$location[fields_df$location == ''] <-
          NA

        fields_df$idCountry[fields_df$idCountry == ''] <-
          NA

        fields_df$idADM1Code[fields_df$idADM1Code == ''] <-
          NA

        fields_df$idFeature[fields_df$idFeature == ''] <-
          NA

        fields_df <-
          fields_df %>%
          dplyr::left_join(data_frame(
            idTypeLocation = 1:5,
            typeLocation = c(
              'country',
              'usState',
              'usCity',
              'wordCity',
              'worldState'
            )
          )) %>%
          dplyr::select(codeGKGTheme:idTypeLocation,
                        typeLocation,
                        everything()) %>%
          suppressMessages()

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticle.field) %>%
            arrange(idArticle.field) %>%
            unite(item, item, idArticle.field, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(as.numeric),
                                vars =
                                  field_data %>% dplyr::select(matches("countEvent")) %>% names) %>%
            dplyr::mutate_each_(funs(as.numeric(., digits = 5)),
                                vars =
                                  field_data %>% dplyr::select(matches("latitude|longitude")) %>% names)

        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticle.field, everything())
        }
      }

      return(field_data)
    }

  if (!count_column %in% names(gdelt_data)) {
    stop("Sorry missing count column")
  }

  col_names <-
    c('idGKG', count_column)

  counts_data <-
    gdelt_data %>%
    dplyr::select_(.dots = col_names)

  names(counts_data)[2] <-
    'count_col'

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!count_col %>% is.na())
  }

  all_counts <-
    1:length(counts_data$count_col) %>%
    purrr::map(function(x) {
      parse_field_count(field = counts_data$count_col[x],
                        return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  if (count_column == "counts") {
    all_counts <-
      all_counts %>%
      dplyr::select(-charLoc)
  }

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  return(all_counts)
}

#' Returns mentioned locations from a gkg data frame
#'
#' @param gdelt_data
#' @param location_column options \code{c('location', 'locations', 'locationsCharLoc', 'locationCharLoc', 'charLoc'))}
#' @param isCharLoc
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_locations <- function(gdelt_data,
                                        location_column = 'locations',
                                        isCharLoc = F,
                                        filter_na = T,
                                        return_wide = F) {
  location_cols <-
    c('location',
      'locations',
      'locationsCharLoc',
      'locationCharLoc',
      'charLoc')

  if (!location_column %in% location_cols) {
    stop("Sorry location column can only be\n" %>%
           paste0(paste0(location_cols, collapse = '\n')))
  }

  if (location_column %in% c('location', 'locations')) {
    location_column <-
      'locations'
  }

  if (location_column %in% c('locationsCharLoc', 'locationCharLoc', 'charLoc')) {
    location_column <-
      'locationsCharLoc'
  }
  parse_location_count <-
    function(field = "4#Leichhardt, New South Wales, Australia#AS#AS02#4944#-33.8833#151.15#-1583352#203;4#Daintree, Queensland, Australia#AS#AS04#40202#-1625#145.317#-1568710#421;4#Daintree, Queensland, Australia#AS#AS04#40202#-1625#145.317#-1568710#2224",
             return_wide = F) {
      options(scipen = 99999, digits = 5)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(location = NA)
        } else {
          field_data <-
            data_frame(location = NA, idArticle.location = 1)
        }
      }  else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(field_item = fields) %>%
          dplyr::mutate(idArticle.location = 1:n())


        if (isCharLoc == T) {
          fields_df <-
            fields_df %>%
            separate(
              col = field_item,
              sep = '\\#',
              into = c(
                'idTypeLocation',
                'location',
                'idCountry',
                'idADM1Code',
                'idADM2Code',
                'latitude',
                'longitude',
                'idFeature',
                'charLoc'
              )
            ) %>%
            suppressMessages() %>%
            suppressWarnings()

        } else {
          fields_df <-
            fields_df %>%
            separate(
              col = field_item,
              sep = '\\#',
              into = c(
                'idTypeLocation',
                'location',
                'idCountry',
                'idADM1Code',
                'latitude',
                'longitude',
                'idFeature'
              )
            ) %>%
            suppressMessages() %>%
            suppressWarnings()
        }

        fields_df <-
          fields_df %>%
          dplyr::mutate_each_(funs(as.numeric),
                              vars =
                                fields_df %>% dplyr::select(matches("idTypeLocation|charLoc")) %>% names) %>%
          dplyr::mutate_each_(funs(as.numeric(., digits = 5)),
                              vars =
                                fields_df %>% dplyr::select(matches("latitude|longitude")) %>% names) %>%
          dplyr::left_join(data_frame(
            idTypeLocation = 1:5,
            typeLocation = c(
              'country',
              'usState',
              'usCity',
              'wordCity',
              'worldState'
            )
          )) %>%
          suppressMessages() %>%
          dplyr::select(idTypeLocation, typeLocation, everything()) %>%
          suppressWarnings()


        fields_df$location[fields_df$location == ''] <-
          NA

        fields_df$idCountry[fields_df$idCountry == ''] <-
          NA

        fields_df$idADM1Code[fields_df$idADM1Code == ''] <-
          NA

        fields_df$idFeature[fields_df$idFeature == ''] <-
          NA

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticle.location) %>%
            arrange(idArticle.location) %>%
            unite(item, item, idArticle.location, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(as.numeric),
                                vars =
                                  field_data %>% dplyr::select(matches("idTypeLocation")) %>% names) %>%
            dplyr::mutate_each_(funs(as.numeric(., digits = 5)),
                                vars =
                                  field_data %>% dplyr::select(matches("latitude|longitude")) %>% names)

        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticle.location, everything())
        }
      }

      return(field_data)
    }

  if (!location_column %in% names(gdelt_data)) {
    stop("Sorry missing location column")
  }

  col_names <-
    c('idGKG', location_column)

  counts_data <-
    gdelt_data %>%
    dplyr::select_(.dots = col_names)

  names(counts_data)[2] <-
    'loc_col'

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!loc_col %>% is.na())
  }

  all_counts <-
    1:length(counts_data$loc_col) %>%
    purrr::map(function(x) {
      parse_location_count(field = counts_data$loc_col[x],
                           return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  if (filter_na == T) {
    all_counts <-
      all_counts %>%
      dplyr::filter(!location %>% is.na())
  }
  return(all_counts)
}

#' Returns mentioned dates from gkg data frame
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_dates <- function(gdelt_data,
                                    filter_na = T,
                                    return_wide = F) {
  parse_dates <-
    function(field = "4#6#16#0#734;4#4#26#0#2258",
             return_wide = F) {
      options(scipen = 99999, digits = 5)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(idDate_resolution = NA)
        } else {
          field_data <-
            data_frame(idDate_resolution = NA,
                       idDateArticle = NA)
        }
      }  else {
        fields <-
          field %>%
          str_split('\\;') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(field_item = fields) %>%
          dplyr::mutate(idDateArticle = 1:n()) %>%
          separate(
            col = field_item,
            sep = '\\#',
            into = c('idDate_resolution', 'month', 'day', 'year', 'charLoc')
          ) %>%
          suppressMessages() %>%
          suppressWarnings()

        fields_df <-
          fields_df %>%
          dplyr::mutate_each_(funs(as.numeric),
                              vars =
                                fields_df  %>% names) %>%
          dplyr::left_join(data_frame(
            idDate_resolution = 1:4,
            date_resolution = c(
              'ex_mon_date',
              'year_only',
              'month_date' ,
              'fully_resolved'
            )
          )) %>%
          suppressMessages() %>%
          dplyr::select(idDate_resolution, date_resolution, everything())

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idDateArticle) %>%
            arrange(idDateArticle) %>%
            unite(item, item, idDateArticle, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(as.numeric),
                                vars =
                                  field_data %>% dplyr::select(
                                    matches("idDate_resolution|month|day|year|charLoc")
                                  ) %>% names)

        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idDateArticle, everything())
        }
      }

      return(field_data)
    }

  if (!'dates' %in% names(gdelt_data)) {
    stop("Sorry missing date column")
  }
  counts_data <-
    gdelt_data %>%
    dplyr::select(idGKG, dates)

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!dates %>% is.na)
  }

  all_counts <-
    1:length(counts_data$dates) %>%
    purrr::map(function(x) {
      parse_dates(field = counts_data$dates[x], return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())


  return(all_counts)
}

#' Returns mentioned quotes from a gkg data frame
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_quotes <- function(gdelt_data,
                                     filter_na = T,
                                     return_wide = F) {
  parse_quotes <-
    function(field = "495|51||knowingly aided and abetted an international kidnap#865|50||nothing less than an international child abduction#2764|49|| staff member should be singled out for dismissal#3373|48||make any serious attempt to independently verify#4059|46||wants to go through every single little detail#4802|156||And xC2 ; xA0 ; you're keeping all of xC2 ; xA0 ; them xC2 ; xA0 ; - xC2 ; xA0 ; except one sacrificial lamb - xC2 ; xA0 ; to run the show?#4879|28||How do you think that looks?#6093|60||an extraordinary conspiracy to remove the children illegally#6150|50||nothing less than an international child abduction#6828|408||I xE2 ; x80 ; xA6 ; have found nothing that supports a finding that any Australian Government official somehow knowingly assisted the mother to do something that was wrong xE2 ; x80 ; xA6 ; I do not find xE2 ; x80 ; xA6 ; that any Australian Embassy officials who helped the mother did so knowing that the mother did not have the father consent to remove the girls permanently from Italy",
             return_wide = F) {
      options(scipen = 99999, digits = 5)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(idArticle.quote = NA)
        } else {
          field_data <-
            data_frame(quote = NA,
                       idArticle.quote = NA)
        }
      }  else {
        fields <-
          field %>%
          str_split('\\#') %>%
          flatten_chr() %>%
          .[!. %in% '']

        fields_df <-
          data_frame(quote_items = fields) %>%
          dplyr::mutate(idArticle.quote = 1:n()) %>%
          separate(
            col = quote_items,
            sep = '\\|',
            into = c('charLoc', 'length.quote', 'verb.intro', 'quote')
          ) %>%
          suppressMessages() %>%
          suppressWarnings()

        fields_df <-
          fields_df %>%
          dplyr::mutate(quote = quote %>% str_trim) %>%
          dplyr::mutate_each_(funs(as.numeric),
                              vars =
                                c('charLoc', 'length.quote')) %>%
          dplyr::select(idArticle.quote, everything())

        fields_df$verb.intro[fields_df$verb.intro == ''] <-
          NA

        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -idArticle.quote) %>%
            arrange(idArticle.quote) %>%
            unite(item, item, idArticle.quote, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(as.numeric),
                                vars =
                                  field_data %>% dplyr::select(matches("charLoc|length.quote")) %>% names)

        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticle.quote, everything())
        }
      }

      return(field_data)
    }

  if (!'quotations' %in% names(gdelt_data)) {
    stop("Sorry missing quotations column")
  }
  counts_data <-
    gdelt_data %>%
    dplyr::select(idGKG, quotations)

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!quotations %>% is.na())
  }

  all_counts <-
    1:length(counts_data$quotations) %>%
    purrr::map(function(x) {
      parse_quotes(field = counts_data$quotations[x], return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows

  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  return(all_counts)
}

#' Returns GCAM codes from a gkg data frame
#'
#' @param gdelt_data
#' @param merge_gcam_codes
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_gcams <- function(gdelt_data,
                                    merge_gcam_codes = F,
                                    filter_na = T,
                                    return_wide = F) {
  parse_gcam_data <-
    function(field = "wc:284,c121:5",
             return_wide = F) {
      options(scipen = 99999)
      if (field %>% is.na) {
        if (return_wide == T) {
          field_data <-
            data_frame(idGCAM = NA)
        } else {
          field_data <-
            data_frame(idGCAM = NA,
                       idArticleGCAM = 1)
        }
      }  else {
        fields <-
          field %>%
          str_split('\\,') %>%
          flatten_chr() %>%
          .[!. %in% '']

        articleWordCount <-
          fields[1] %>%
          extract_numeric()

        fields_df <-
          data_frame(articleWordCount,
                     idGCAM = fields[2:length(fields)]) %>%
          separate(idGCAM,
                   into = c('idGCAM', 'scoreGoldsteinWords'),
                   sep = '\\:') %>%
          dplyr::mutate(
            idArticleGCAM = 1:n(),
            scoreGoldsteinWords = scoreGoldsteinWords %>% as.numeric(., digits = 4)
          )


        if (return_wide == T) {
          fields_df <-
            fields_df %>%
            gather(item, value, -c(articleWordCount, idArticleGCAM)) %>%
            arrange(idArticleGCAM) %>%
            unite(item, item, idArticleGCAM, sep = '.')

          order_fields <-
            fields_df$item

          field_data <-
            fields_df %>%
            spread(item, value) %>%
            dplyr::select_(.dots = order_fields)

          field_data <-
            field_data %>%
            dplyr::mutate_each_(funs(as.numeric),
                                vars =
                                  field_data %>% dplyr::select(matches("scoreGoldsteinWords")) %>% names)

        } else {
          field_data <-
            fields_df

          field_data <-
            field_data %>%
            dplyr::select(idArticleGCAM, everything())
        }
      }

      return(field_data)
    }

  if (!'gcam' %in% names(gdelt_data)) {
    stop("Sorry missing video embed column")
  }
  counts_data <-
    gdelt_data %>%
    dplyr::select(idGKG, gcam)

  if (filter_na == T) {
    counts_data <-
      counts_data %>%
      dplyr::filter(!gcam %>% is.na())
  }

  all_counts <-
    1:length(counts_data$gcam) %>%
    purrr::map(function(x) {
      parse_gcam_data(field = counts_data$gcam[x],
                      return_wide = return_wide) %>%
        dplyr::mutate(idGKG = counts_data$idGKG[x])
    }) %>%
    purrr::compact %>%
    bind_rows
  if (merge_gcam_codes == T) {
    all_counts <-
      all_counts %>%
      dplyr::left_join(
        get_codes_gcam() %>%
          dplyr::select(idGCAM, type, dictonary.human_name,
                        dimension.human_name)
      )
  }
  all_counts <-
    all_counts %>%
    dplyr::select(idGKG, everything())

  return(all_counts)
}

#' Returns source name or source url from a gkg data frame
#'
#' @param gdelt_data
#' @param source_column options \code{c('sources', 'source', 'sources.url', 'source.url'))}
#' @param filter_na
#' @param return_wide
#' @importFrom urltools domain
#'
#' @return
#' @export
#'
#' @examples
get_mentioned_gkg_source_data <-
  function(gdelt_data,
           source_column = 'sources',
           filter_na = T,
           return_wide = F) {
    source_options <-
      c('sources', 'source', 'urlSources', 'urlSource')
    if (!source_column %in% source_options) {
      stop("Sorry source column can only be\n" %>%
             paste0(paste0(source_options, collapse = '\n')))
    }

    if (source_column %in% c('sources', 'source')) {
      source_column <-
        'sources'
    }

    if (source_column %in% c('urlSources', 'urlSource')) {
      source_column <-
        'urlSources'
    }
    parse_source_name <-
      function(field = "businesstimes.com.sg;businesstimes.com.sg",
               return_wide = F) {
        options(scipen = 99999)
        if (field %>% is.na) {
          if (return_wide == T) {
            field_data <-
              data_frame(nameSource = NA)
          } else {
            field_data <-
              data_frame(nameSource = NA,
                         idArticle.source1 = 1)
          }
        }  else {
          fields <-
            field %>%
            str_split('\\;') %>%
            flatten_chr() %>%
            .[!. %in% ''] %>%
            unique

          fields_df <-
            data_frame(nameSource = fields) %>%
            dplyr::mutate(idArticle.source = 1:n())
          if (return_wide == T) {
            fields_df <-
              fields_df %>%
              gather(item, value, -idArticle.source) %>%
              arrange(idArticle.source) %>%
              unite(item, item, idArticle.source, sep = '.')

            order_fields <-
              fields_df$item

            field_data <-
              fields_df %>%
              spread(item, value) %>%
              dplyr::select_(.dots = order_fields)

          } else {
            field_data <-
              fields_df

            field_data <-
              field_data %>%
              dplyr::select(idArticle.source,
                            everything())
          }
        }

        return(field_data)
      }

    if (!source_column %in% names(gdelt_data)) {
      stop("Sorry missing source column")
    }

    col_names <-
      c('idGKG', source_column)

    counts_data <-
      gdelt_data %>%
      dplyr::select_(.dots = col_names)

    names(counts_data)[2] <-
      'source_col'

    if (filter_na == T) {
      counts_data <-
        counts_data %>%
        dplyr::filter(!source_col %>% is.na())
    }

    all_counts <-
      1:length(counts_data$source_col) %>%
      purrr::map(function(x) {
        parse_source_name(field = counts_data$source_col[x],
                          return_wide = return_wide) %>%
          dplyr::mutate(idGKG = counts_data$idGKG[x])
      }) %>%
      purrr::compact %>%
      bind_rows

    if (source_column == 'urlSources') {
      names(all_counts)[2] <-
        c('urlSource')
    }

    if (source_column == 'sources') {
      names(all_counts)[2] <-
        c('nameSource')
    }

    all_counts <-
      all_counts %>%
      dplyr::select(idGKG, everything())

    return(all_counts)
  }


#' Retrieves detailed GKG data for a given day from a specified table
#'
#' @param date_data must be a date in Year - Month - Day format
#' @param table_name options \code{c('gkg', 'export', 'mentions'))}
#' @param file_directory
#' @param empty_trash
#' @param return_message
#' @return
#'
#' @examples

get_data_gkg_day_detailed <- function(date_data = "2016-06-01",
                                      table_name = "gkg",
                                      file_directory = 'Desktop/temp_gdelt_data',
                                      only_most_recent = F,
                                      remove_files = T,
                                      empty_trash = T,
                                      return_message = T) {
  if (only_most_recent == T) {
    date_data <-
      Sys.Date()
  }

  if (!date_data %>% substr(5, 5) == "-") {
    stop("Sorry data must be in YMD format, ie, 2016-06-01")
  }
  tables <-
    c('gkg', 'export', 'mentions')
  if (!table_name %in% tables) {
    stop("Sorry tables can only be:\n" %>% paste0(paste0(tables, collapse = '\n')))
  }

  date_data <-
    date_data %>%
    ymd %>% as.Date()


  if (date_data < "2015-02-18") {
    stop("Sorry data starts on February 18th, 2015")
  }

  if (date_data > Sys.Date()) {
    stop("Sorry data can't go into the future")
  }

  if (only_most_recent == T) {
    gdelt_detailed_logs <-
      get_urls_gkg_most_recent_log()
    urls <-
      gdelt_detailed_logs %>%
      dplyr::filter(nameFile == table_name) %>%
      .$urlData
  } else {
    if (!'gdelt_detailed_logs' %>% exists) {
      paste("To save memory and time next time you should run the function get_urls_gkg_15_minute_log and save to data frame called gdelt_detailed_logs") %>%
        message
      gdelt_detailed_logs <-
        get_urls_gkg_15_minute_log()
    }
    urls <-
      gdelt_detailed_logs %>%
      dplyr::filter(dateData == date_data) %>%
      dplyr::filter(nameFile == table_name) %>%
      .$urlData
  }

  get_gdelt_url_data_safe <-
    failwith(NULL, get_gdelt_url_data)

  all_data <-
    urls %>%
    purrr::map(function(x) {
      get_gdelt_url_data_safe(
        url = x,
        remove_files = remove_files,
        file_directory = file_directory,
        return_message = return_message,
        empty_trash = empty_trash
      )
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    distinct %>%
    suppressMessages() %>%
    suppressWarnings()

  if (return_message == T) {
    "You retrieved " %>%
      paste0(all_data %>% nrow, " gkg detailed events for ", date_data) %>%
      message()
  }

  return(all_data)
}


#' Get dates detailed data from a specified table
#'
#' @param dates
#' @param table_name
#' @param file_directory
#' @param remove_files
#' @param empty_trash
#' @param return_message
#' @importFrom purrr map
#' @return
#' @export
#'
#' @examples
get_data_gkg_days_detailed <- function(dates = c("2016-06-01"),
                                       table_name = "gkg",
                                       file_directory = 'Desktop/temp_gdelt_data',
                                       only_most_recent = F,
                                       remove_files = T,
                                       empty_trash = T,
                                       return_message = T) {
  get_data_gkg_day_detailed_safe <-
    failwith(NULL, get_data_gkg_day_detailed)

  all_data <-
    dates %>%
    purrr::map(
      function(x)
        get_data_gkg_day_detailed_safe(
          date_data = x,
          table_name = table_name,
          only_most_recent = only_most_recent,
          file_directory = file_directory,
          remove_files = remove_files,
          empty_trash = empty_trash,
          return_message = return_message
        )
    ) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  all_data <-
    all_data %>%
    dplyr::rename(idDateTime = idDate_timeArticle) %>%
    dplyr::mutate(idDateTime = 1:n()) %>%
    separate(idGKG, sep = '\\-', c('dateCode', 'remove')) %>%
    unite(idGKG, dateCode, idDateTime, sep = '-',remove = F) %>%
    dplyr::select(-c(dateCode, remove))

  return(all_data)
}

#' Retrieves gkg summary file for a given day
#'
#' @param date_data
#' @param file_directory
#' @param is_count_file options \code{c(TRUE, FALSE)}
#' @param remove_files
#' @param empty_trash
#' @param return_message
#'
#' @return
#'
#' @examples

get_data_gkg_day_summary <- function(date_data = "2016-06-01",
                                     file_directory = 'Desktop/temp_gdelt_data',
                                     is_count_file = F,
                                     remove_files = T,
                                     empty_trash = T,
                                     return_message = T) {
  options(scipen = 99999)
  if (!date_data %>% substr(5, 5) == "-") {
    stop("Sorry data must be in YMD format, ie, 2016-06-01")
  }

  date_data <-
    date_data %>%
    ymd %>% as.Date()


  if (date_data < "2013-04-01") {
    stop("Sorry data starts on April 1st, 2013")
  }

  if (date_data > Sys.Date()) {
    stop("Sorry data can't go into the future")
  }
  if (!'summary_data_urls' %>% exists) {
    paste0("To save time and memory next time you should run the function get_urls_gkg_daily_summaries and save it to a data frame called summary_data_urls") %>% message
    summary_data_urls <-
      get_urls_gkg_daily_summaries(return_message = return_message)
  }

  if (is_count_file == T) {
    summary_data_urls <-
      summary_data_urls %>%
      dplyr::filter(isCountFile == T)
  } else {
    summary_data_urls <-
      summary_data_urls %>%
      dplyr::filter(isCountFile == F)
  }

  urls <-
    summary_data_urls %>%
    dplyr::filter(dateData == date_data) %>%
    .$urlData

  get_gdelt_url_data_safe <-
    failwith(NULL, get_gdelt_url_data)

  all_data <-
    urls %>%
    purrr::map(function(x) {
      get_gdelt_url_data_safe(
        url = x,
        remove_files = remove_files,
        file_directory = file_directory,
        return_message = return_message,
        empty_trash = empty_trash
      )
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    distinct

  if ('countObject' %in% names(all_data)) {
    all_data <-
      all_data %>%
      dplyr::mutate(countObject = countObject %>% as.numeric())
  }
  if ('idCAMEOEvents' %in% names(all_data)) {
    all_data <-
      all_data %>%
      mutate(idCAMEOEvents = idCAMEOEvents %>% as.character())
  }


  if (return_message == T) {
    "You retrieved " %>%
      paste0(all_data %>% nrow, " gkg summary events for ", date_data) %>%
      message()
  }

  return(all_data)
}

#' Gets days summary GDELT GKG data by table
#'
#' @param dates
#' @param table_name
#' @param file_directory
#' @param remove_files
#' @param empty_trash
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
#' get_data_gkg_days_summary(dates = c("2016-06-01"), is_count_file = F)

get_data_gkg_days_summary <- function(dates = c("2016-06-01"),
                                      is_count_file = F,
                                      file_directory = 'Desktop/temp_gdelt_data',
                                      remove_files = T,
                                      empty_trash = T,
                                      return_message = T) {
  get_data_gkg_day_summary_safe <-
    failwith(NULL, get_data_gkg_day_summary)

  all_data <-
    dates %>%
    map(
      function(x)
        get_data_gkg_day_summary_safe(
          date_data = x,
          is_count_file = is_count_file,
          file_directory = file_directory,
          remove_files = remove_files,
          empty_trash = empty_trash,
          return_message = return_message
        )
    ) %>%
    purrr::compact %>%
    bind_rows

  return(all_data)
}

#' Retreive GDELT data for a given period
#'
#' @param period
#' @param file_directory
#' @param is_count_file
#' @param remove_files
#' @param empty_trash
#' @param return_message
#' @importFrom purrr compact
#' @return
#'
#' @examples
get_data_gdelt_period_event <- function(period = 1983,
                                        file_directory = 'Desktop/temp_gdelt_data',
                                        remove_files = T,
                                        empty_trash = T,
                                        return_message = T) {
  period <-
    period %>%
    as.character()

  if (!'gdelt_event_urls' %>% exists) {
    paste0("To save memory and time you want to run the function get_urls_gdelt_event_log into a data_frame called gdelt_event_urls") %>%
      message()
    gdelt_event_urls <-
      get_urls_gdelt_event_log(return_message = return_message)
  }
  periods <-
    gdelt_event_urls$periodData
  if (!period %in% periods) {
    gdelt_event_message <-
      "Period can only be a 4 digit year between 1979 and 2005\nEXAMPLE: 1983\nA 6 digit year and month from 2006 to March 2013\nEXAMPLE: 201208\nOr an 8 digit year, month, day from March 1, 2013 until today\nEXAMPLE: 20140303"

    stop(gdelt_event_message)
  }

  urls <-
    gdelt_event_urls %>%
    dplyr::filter(periodData == period) %>%
    .$urlData

  get_gdelt_url_data_safe <-
    failwith(NULL, get_gdelt_url_data)

  all_data <-
    urls %>%
    purrr::map(function(x) {
      get_gdelt_url_data_safe(
        url = x,
        remove_files = remove_files,
        file_directory = file_directory,
        return_message = return_message,
        empty_trash = empty_trash
      )
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    distinct

  if (return_message == T) {
    "You retrieved " %>%
      paste0(all_data %>% nrow, " GDELT events for the period of ", period) %>%
      message()
  }

  return(all_data)
}


#' Returns GDELT event data for a given periods
#'
#' @param periods
#' @param file_directory
#' @param is_count_file
#' @param remove_files
#' @param empty_trash
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
#' get_data_gdelt_periods_event (periods = c(1983))
get_data_gdelt_periods_event <- function(periods = c(1983, 1984),
                                         file_directory = 'Desktop/temp_gdelt_data',
                                         remove_files = T,
                                         empty_trash = T,
                                         return_message = T) {
  get_data_gdelt_period_event_safe <-
    failwith(NULL, get_data_gdelt_period_event)

  all_data <-
    1:length(periods) %>%
    map(
      function(x)
        get_data_gdelt_period_event_safe(
          period = periods[x],
          file_directory = file_directory,
          remove_files = remove_files,
          empty_trash = empty_trash,
          return_message = return_message
        )
    ) %>%
    purrr::compact %>%
    bind_rows

  return(all_data)
}

#' get_cloud_vision_schema
#'
#' @return
#' @importFrom dplyr data_frame
#'
#' @examples
get_cloud_vision_schema  <- function() {
  cv_schema <-
    data_frame(
      nameGDELT = c(
        "DATE",
        "DocumentIdentifier",
        "ImageURL",
        "Labels",
        "GeoLandmarks",
        "Logos",
        "SafeSearch",
        "Faces",
        "OCR",
        "LangHints",
        "WidthHeight",
        "RawJSON"
      ),
      nameActual =
        c(
          "date_timeURL",
          "documentSource",
          "urlImage",
          "xmlLabels",
          "xmlGeoLandmarks",
          "xmlLogos",
          "xmlSafeSearch",
          "xmlFaces",
          "xmlOCR",
          "codesLanguages",
          "dimWidthHeight",
          "jsonCloudVision"
        )

    )
  return(cv_schema)
}

#' get_urls_cv
#'
#' @return
#' @export
#' @importFrom lubridate ymd_hms
#' @importFrom dplyr data_frame
#' @examples
get_urls_cv <- function() {
  options(scipen = 999999)
  cloud_vision_start_hms <-
    20160222113000

  time_now <-
    Sys.time() %>% format("%Y%m%d%H%M%S") %>% as.numeric()

  all_dates <-
    seq(ymd_hms(cloud_vision_start_hms),
        to = time_now %>% ymd_hms,
        by = '15 min')

  url_df <-
    data_frame(
      date_timeData = all_dates,
      isoPeriod = date_timeData %>% format("%Y%m%d%H%M%S") %>% as.numeric(),
      urlCloudVisionTags = 'http://data.gdeltproject.org/gdeltv2_cloudvision/' %>% paste0(isoPeriod, '.imagetagsv1.csv.gz'),
      urlTranslationTags = 'http://data.gdeltproject.org/gdeltv2_cloudvision/' %>% paste0(isoPeriod, '.translation.imagetagsv1.csv.gz')
    ) %>%
    mutate(dateData = date_timeData %>% as.Date()) %>%
    arrange(desc(date_timeData))
  return(url_df)
}

#' Gets most recent CV log URLs
#'
#' @return
#' @import stringr
#' @import dplyr
#' @importFrom magrittr %>%
#' @importFrom readr read_tsv
#' @importFrom tidyr spread
#'
#' @examples get_urls_gkg_most_recent_log()
get_urls_cv_most_recent  <- function() {
  log_df <-
    'http://data.gdeltproject.org/gdeltv2_cloudvision/lastupdate.txt' %>%
    readr::read_tsv(col_names = F)

  names(log_df) <-
    c('value')

  log_df <-
    log_df %>%
    mutate(item = c('urlCloudVisionTags', 'urlTranslationTags')) %>%
    spread(item, value)

  return(log_df)
}


#' Gets Cloud Vision Data File
#'
#' @param url
#' @param file_directory
#' @param remove_files
#' @param empty_trash
#' @param return_message
#' @export
#' @return
#' @importFrom httr url_ok
#' @import stringr
#' @importFrom purrr flatten_chr
#' @importFrom curl curl_download
#' @importFrom readr read_tsv
#' @importFrom lubridate ymd_hms
#' @importFrom lubridate with_tz
#' @examples
get_data_cv_url <-
  function(url = 'http://data.gdeltproject.org/gdeltv2_cloudvision/20160606234500.imagetagsv1.csv.gz',
           file_directory = 'Desktop/temp_gdelt_data',
           remove_files = T,
           empty_trash = T,
           return_message = T) {
    ok_url <-
      url %>% httr::url_ok %>% suppressWarnings()
    if (ok_url == FALSE) {
      stop("Invalid url")
    }

    files <-
      url %>%
      str_replace_all(
        'http://data.gdeltproject.org/gdeltv2/|http://data.gdeltproject.org/gkg/|http://data.gdeltproject.org/events/|http://data.gdeltproject.org/gdeltv2_cloudvision/',
        ''
      ) %>%
      str_split('\\.') %>%
      flatten_chr

    file_name <-
      files %>%
      paste0(collapse = '.')

    temp.dir <-
      file_directory

    file_path <-
      temp.dir %>% str_split('/') %>% flatten_chr() %>% .[1:length(.)] %>% paste0(collapse = '/')

    if (dir.exists(paths = file_path)) {
      "rm -R " %>%
        paste0(temp.dir) %>%
        system()
      if (empty_trash == T) {
        system('rm -rf ~/.Trash/*')
      }
    }

    if (!dir.exists(paths = file_path)) {
      dir.create(temp.dir)
    }

    file <-
      temp.dir %>%
      paste0('/', file_name)

    url %>%
      curl_download(url = ., destfile = file)

    dir_files <-
      temp.dir %>%
      list.files()

    csv_file_loc <-
      dir_files[dir_files %>%
                  str_detect(".csv|.CSV")] %>%
      paste0(temp.dir, '/', .)

    cloud_vision_data <-
      csv_file_loc %>%
      gzfile() %>%
      read_tsv(col_names = F) %>%
      suppressWarnings()

    names(cloud_vision_data) <-
      get_cloud_vision_schema() %>% .$nameActual

    cloud_vision_data <-
      cloud_vision_data %>%
      dplyr::mutate(
        date_codeURL = date_timeURL,
        date_timeURL = date_timeURL %>% lubridate::ymd_hms() %>% with_tz(Sys.timezone()),
        dateURL = date_timeURL %>% as.Date,
        idDateTime = 1:n(),
        dimWidthHeight = dimWidthHeight %>% tidyr::extract_numeric
      )

    if (remove_files == T) {
      "rm -R " %>%
        paste0(temp.dir) %>%
        system()
      if (empty_trash == T) {
        system('rm -rf ~/.Trash/*')
      }
    }

    if (return_message) {
      "Downloaded, parsed and imported " %>%
        paste0(url) %>%
        message()

    }
    return(cloud_vision_data)
  }

#' Get Data CV Day
#'
#' @param date_data
#' @param include_translations
#' @param file_directory
#' @param remove_files
#' @param empty_trash
#' @param return_message
#'
#' @return
#'
#' @examples
get_data_cv_day <-
  function(date_data = "2016-06-08",
           include_translations = F,
           only_most_recent = F,
           file_directory = 'Desktop/temp_gdelt_data',
           remove_files = T,
           empty_trash = T,
           return_message = T) {

    if (!date_data %>% substr(5, 5) == "-") {
      stop("Sorry data must be in YMD format, ie, 2016-06-01")
    }

    date_data <-
      date_data %>%
      ymd %>% as.Date()


    if (date_data < "2016-02-22") {
      stop("Sorry data starts on February 22, 2016")
    }

    if (date_data > Sys.Date()) {
      stop("Sorry data can't go into the future")
    }

    if (!'cb_urls' %>% exists) {
      paste("To save memory and time next time you should run the function get_urls_cv()  and save to data frame called cv_urls") %>%
        message
      cv_urls <-
        get_urls_cv()
    }
    urls <-
      cv_urls %>%
      dplyr::filter(dateData == date_data) %>%
      .$urlCloudVisionTags

    if (include_translations){
      urls <-
        c(urls,
          cv_urls %>%
            dplyr::filter(dateData == date_data) %>%
            .$urlTranslationTags)
    }

    if( only_most_recent == T) {
      urls <-
        get_urls_cv_most_recent() %>%
        .$urlCloudVisionTags
    }
    get_data_cv_url_safe <-
      failwith(NULL, get_data_cv_url)

    all_data <-
      urls %>%
      purrr::map(function(x) {
        get_data_cv_url_safe(
          url = x,
          remove_files = remove_files,
          file_directory = file_directory,
          return_message = return_message,
          empty_trash = empty_trash
        )
      }) %>%
      purrr::compact %>%
      bind_rows %>%
      distinct %>%
      dplyr::select(idDateTime,date_timeURL, everything()) %>%
      suppressWarnings()

    if (return_message == T) {
      "You retrieved " %>%
        paste0(all_data %>% nrow, " cloud vision processed items for ", date_data) %>%
        message()
    }

    return(all_data)

  }

#' Gets CV Data for stated periods
#'
#' @param date_data
#' @param include_translations
#' @param file_directory
#' @param remove_files
#' @param empty_trash
#' @param return_message
#'
#' @return
#' @export
#'
#' @examples
get_data_cv_dates <-
  function(dates = c("2016-06-09", "2016-06-08"),
           include_translations = F,
           file_directory = 'Desktop/temp_gdelt_data',
           only_most_recent = F,
           remove_files = T,
           empty_trash = T,
           return_message = T) {
    if (only_most_recent == T) {
      dates <-
        Sys.Date()
    }
    get_data_cv_day_safe <-
      failwith(NULL, get_data_cv_day)

    all_data <-
      dates %>%
      map(
        function(x)
          get_data_cv_day_safe(
            date_data = x,
            file_directory = file_directory,
            only_most_recent = only_most_recent,
            remove_files = remove_files,
            empty_trash = empty_trash,
            return_message = return_message
          )
      ) %>%
      purrr::compact %>%
      bind_rows

    all_data <-
      all_data %>%
      mutate(idDateTime = 1:n()) %>%
      distinct()

    return(all_data)

  }

#' Parse XML Labels
#'
#' @param data
#' @param id_date_time
#'
#' @return
#' @import dplyr
#' @import tidyr
#' @import xml2
#' @importFrom purrr safely
#'
#' @examples
parse_xml_extras <-
  function(data, id_date_time = 5) {
    xmlData <-
      data %>%
      dplyr::filter(idDateTime == id_date_time) %>%
      .$xmlExtras


    if (xmlData %>% is.na()) {
      xml_df <-
        data_frame(idDateTime = id_date_time)
    } else {
      safely_read_xml <-
        safely(read_xml)

      xml_results <-
        "<article_xml>" %>%
        paste0(xmlData, "</article_xml>") %>%
        safely_read_xml()

      if (xml_results$result %>% is.null()) {
        xml_df <-
          data_frame(idDateTime = id_date_time)
      } else {
        items <-
          xml_results[[1]] %>% xml_children() %>% xml_name %>% str_to_lower()

        values <-
          xml_results[[1]] %>% xml_children() %>% xml_text()

        xml_df <-
          data_frame(idDateTime = id_date_time, item = items, value = values) #%>%
        #spread(item, value)
      }
    }

    return(xml_df)

  }

#' Parses XML Extras
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#' @importFrom purrr compact
#' @importFrom purrr map
#' @examples
parse_gkg_xml_extras <- function(gdelt_data,
                                 filter_na = T,
                                 return_wide = F) {
  parse_xml_extras_safe <-
    failwith(NULL, parse_xml_extras)

  allxmlLabels <-
    gdelt_data$idDateTime %>%
    purrr::map(function(x) {
      parse_xml_extras_safe(data = gdelt_data, id_date_time = x)
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  if( filter_na == T) {
    allxmlLabels <-
      allxmlLabels %>%
      dplyr::filter(!item %>% is.na())
  }

  if (return_wide == T) {
    allxmlLabels <-
      allxmlLabels %>%
      spread(item, value)
  }

  return(allxmlLabels)
}

#' Parse XML Labels
#'
#' @param data
#' @param id_date_time
#'
#' @return
#' @import dplyr
#' @import tidyr
#'
#' @examples
parse_xml_labels <-
  function(data, id_date_time = 1)
  {
    xmlData <-
      data %>%
      dplyr::filter(idDateTime == id_date_time) %>%
      .$xmlLabels

    if (xmlData %>% is.na()) {
      xml_df <-
        data_frame(idDateTime = id_date_time)
    } else {
      if (xmlData %>% str_detect('<RECORD>')) {
        xmlData <-
          xmlData %>%
          str_split('<RECORD>') %>%
          flatten_chr()
      }


      xml_df <-
        data_frame(xmlData) %>%
        separate(
          xmlData,
          into = c('nameLabel', 'scoreConfidenceLabel', 'midGoogle'),
          sep = '<FIELD>'
        ) %>%
        mutate(
          idDateTime = id_date_time,
          idImageLabel = 1:n(),
          scoreConfidenceLabel = scoreConfidenceLabel %>% as.numeric()
        ) %>%
        dplyr::select(idDateTime, idImageLabel, everything()) %>%
        suppressWarnings()
    }

    return(xml_df)

  }

#' Parses Cloud Vision Lables
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#' @importFrom purrr compact
#' @importFrom purrr map
#' @examples
parse_cv_labels <- function(gdelt_data,
                            filter_na = T,
                            return_wide = F) {
  parse_xml_labels_safe <-
    failwith(NULL, parse_xml_labels)

  allxmlLabels <-
    gdelt_data$idDateTime %>%
    purrr::map(function(x) {
      parse_xml_labels_safe(data = gdelt_data, id_date_time = x)
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  return(allxmlLabels)
}


#' Parses XML Landmarks
#'
#' @param data
#' @param id_date_time
#'
#' @return
#'
#' @examples
parse_xml_landmarks <-
  function(data, id_date_time = 886) {
    xmlData <-
      data %>%
      dplyr::filter(idDateTime == id_date_time) %>%
      .$xmlGeoLandmarks

    if (xmlData %>% is.na()) {
      xml_df <-
        data_frame(idDateTime = id_date_time)
    } else {
      if (xmlData %>% str_detect('<RECORD>')) {
        xmlData <-
          xmlData %>%
          str_split('<RECORD>') %>%
          flatten_chr()
      }


      xml_df <-
        data_frame(xmlData) %>%
        separate(
          xmlData,
          into = c(
            'nameLandmark',
            'scoreConfidenceLandmark',
            'midGoogle',
            'latLonLandmark'
          ),
          sep = '<FIELD>'
        ) %>%
        separate(
          col = 'latLonLandmark',
          sep = '\\,',
          into = c('latitudeLandmark', 'longitudeLandmark')
        ) %>%
        mutate(
          idDateTime = id_date_time,
          idLandmarkImage = 1:n(),
          scoreConfidenceLandmark = scoreConfidenceLandmark %>% as.numeric(),
          latitudeLandmark = latitudeLandmark %>% as.numeric,
          longitudeLandmark = longitudeLandmark %>% as.numeric
        ) %>%
        dplyr::select(idDateTime, idLandmarkImage, everything()) %>%
        suppressWarnings()
    }

    return(xml_df)

  }

#' Parses Cloud Vision Landmark
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
parse_cv_landmarks <- function(gdelt_data,
                               filter_na = T,
                               return_wide = F) {
  parse_xml_landmarks_safe <-
    failwith(NULL, parse_xml_landmarks)

  all_data <-
    gdelt_data$idDateTime %>%
    purrr::map(function(x) {
      parse_xml_landmarks_safe(data = gdelt_data, id_date_time = x)
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  if (filter_na == T) {
    all_data <-
      all_data %>%
      dplyr::filter(!nameLandmark %>% is.na)
  }

  return(all_data)
}


#' Parses XML Logo
#'
#' @param data
#' @param id_date_time
#'
#' @return
#'
#' @examples
parse_xml_logos <-
  function(data, id_date_time = 1) {
    xmlData <-
      data %>%
      dplyr::filter(idDateTime == id_date_time) %>%
      .$xmlLogos

    if (xmlData %>% is.na()) {
      xml_df <-
        data_frame(idDateTime = id_date_time)
    } else {
      xmlData <-
        xmlData %>%
        str_split('<RECORD>') %>%
        flatten_chr()

      xml_df <-
        data_frame(xmlData) %>%
        separate(
          xmlData,
          into = c('nameLogo', 'scoreConfidenceLogo', 'midGoogle'),
          sep = '<FIELD>'
        ) %>%
        mutate(
          idDateTime = id_date_time,
          idLogoImage = 1:n(),
          scoreConfidenceLogo = scoreConfidenceLogo %>% as.numeric()
        ) %>%
        dplyr::select(idDateTime, idLogoImage, everything()) %>%
        suppressWarnings()
    }

    return(xml_df)

  }

#' Parses Cloud Vision Logos
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
parse_cv_logos <- function(gdelt_data,
                           filter_na = T,
                           return_wide = F) {
  parse_xml_logos_safe <-
    failwith(NULL, parse_xml_logos)

  all_data <-
    gdelt_data$idDateTime %>%
    purrr::map(function(x) {
      parse_xml_logos_safe(data = gdelt_data, id_date_time = x)
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  all_data <-
    all_data %>%
    mutate(midGoogle = ifelse(midGoogle == '', NA, midGoogle))

  if (filter_na == T) {
    all_data <-
      all_data %>%
      dplyr::filter(!nameLogo %>% is.na)
  }
  return(all_data)
}

#' Parses Safe Search
#'
#' @param data
#' @param id_date_time
#'
#' @return
#'
#' @examples
parse_xml_safe_search <-
  function(data, id_date_time = 1) {
    xmlData <-
      data %>%
      dplyr::filter(idDateTime == id_date_time) %>%
      .$xmlSafeSearch

    if (xmlData %>% is.na()) {
      xml_df <-
        data_frame(idDateTime = id_date_time)
    } else {
      xmlData <-
        xmlData %>%
        str_split('<RECORD>') %>%
        flatten_chr()

      xml_df <-
        data_frame(xmlData) %>%
        separate(
          xmlData,
          into = c(
            'scoreViolenceLikelihood',
            'scoreMedicalLikelihood',
            'scoreSpoofLikelihood',
            'scoreAdultLikelihood'
          ),
          sep = '<FIELD>'
        )

      xml_df <-
        xml_df %>%
        mutate_each_(funs(as.integer),
                     vars = xml_df %>% dplyr::select(matches("score")) %>% names) %>%
        mutate(idDateTime = id_date_time,
               idSafeSearchImage = 1:n()) %>%
        dplyr::select(idDateTime, idSafeSearchImage, everything()) %>%
        suppressWarnings() %>%
        distinct()
    }

    return(xml_df)

  }

#' Parses Safe Search
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
parse_cv_safe_search <- function(gdelt_data,
                                 filter_na = T,
                                 return_wide = F) {
  parse_xml_safe_search_safe <-
    failwith(NULL, parse_xml_safe_search)

  all_data <-
    gdelt_data$idDateTime %>%
    purrr::map(function(x) {
      parse_xml_safe_search_safe(data = gdelt_data, id_date_time = x)
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  if (filter_na == T) {
    all_data <-
      all_data %>%
      dplyr::filter(!idSafeSearchImage %>% is.na)
  }
  return(all_data)
}

#' Parses XML Faces
#'
#' @param data
#' @param id_date_time
#'
#' @return
#'
#' @examples
parse_xml_faces <-
  function(data, id_date_time = 1) {
    xmlData <-
      data %>%
      dplyr::filter(idDateTime == id_date_time) %>%
      .$xmlFaces

    if (xmlData %>% is.na()) {
      xml_df <-
        data_frame(idDateTime = id_date_time)
    } else {
      xmlData <-
        xmlData %>%
        str_split('<RECORD>') %>%
        flatten_chr()

      xml_df <-
        data_frame(xmlData) %>%
        separate(
          xmlData,
          into = c(
            'scoreDetectionConfidence',
            'angleRoll',
            'anglePan',
            'angleTilt',
            'scoreLandmarkingConfidence',
            'boxBounding',
            'scoreEmotionSorrowLikelihood',
            'scoreEmotionAngerLikelihood',
            'scoreHeadwearLikelihood',
            'scoreEmotionJoyLikelihood',
            'scoreEmotionSurpriseLikelihood',
            'scoreUnderExposedLikelihood',
            'scoreBlurredLikelihood'
          ),
          sep = '<FIELD>'
        ) %>%
        separate('boxBounding', sep = ';', c('xy1', 'xy2', 'xy3', 'xy4')) %>%
        separate('xy1', sep = ',', c('faceX1', 'faceY1')) %>%
        separate('xy2', sep = ',', c('faceX2', 'faceY2')) %>%
        separate('xy3', sep = ',', c('faceX3', 'faceY3')) %>%
        separate('xy4', sep = ',', c('faceX4', 'faceY4'))

      xml_df <-
        xml_df %>%
        mutate_each_(funs(as.numeric),
                     vars = xml_df %>% dplyr::select(matches("score|face|angle")) %>% names) %>%
        mutate(idDateTime = id_date_time,
               idFace = 1:n()) %>%
        dplyr::select(idDateTime, idFace, everything()) %>%
        suppressWarnings() %>%
        distinct()
    }

    return(xml_df)

  }

#' Parses Cloud Vision Faces
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
parse_cv_faces <- function(gdelt_data,
                           filter_na = T,
                           return_wide = F) {
  parse_xml_faces_search_safe <-
    failwith(NULL, parse_xml_faces)

  all_data <-
    gdelt_data$idDateTime %>%
    purrr::map(function(x) {
      parse_xml_faces_search_safe(data = gdelt_data, id_date_time = x)
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  if (filter_na == T) {
    all_data <-
      all_data %>%
      dplyr::filter(!scoreDetectionConfidence %>% is.na)
  }
  return(all_data)
}

#' Parses XML OCR
#'
#' @param data
#' @param id_date_time
#'
#' @return
#'
#' @examples
parse_xml_ocr <-
  function(data, id_date_time = 1)  {
    xmlData <-
      data %>%
      dplyr::filter(idDateTime == id_date_time) %>%
      .$xmlOCR

    if (xmlData %>% is.na()) {
      xml_df <-
        data_frame(idDateTime = id_date_time)
    } else {
      xmlData <-
        xmlData %>%
        str_split('<RECORD>') %>%
        flatten_chr() %>%
        gsub('\\n', '', .)

      xml_df <-
        data_frame(textOCR = xmlData)

      xml_df <-
        xml_df %>%
        mutate(idDateTime = id_date_time,
               idItemOCR = 1:n()) %>%
        dplyr::select(idDateTime, idItemOCR, everything()) %>%
        suppressWarnings() %>%
        distinct()
    }

    return(xml_df)

  }

#' Parses Cloud Vision OCR
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
parse_cv_ocr <- function(gdelt_data,
                         filter_na = T,
                         return_wide = F) {
  parse_xml_ocr_safe <-
    failwith(NULL, parse_xml_ocr)

  all_data <-
    gdelt_data$idDateTime %>%
    purrr::map(function(x) {
      parse_xml_ocr_safe(data = gdelt_data, id_date_time = x)
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  if (filter_na == T) {
    all_data <-
      all_data %>%
      dplyr::filter(!idItemOCR %>% is.na)
  }
  return(all_data)
}


#' Parses XML Language Type
#'
#' @param data
#' @param id_date_time
#'
#' @return
#'
#' @examples
parse_language_types <-
  function(data, id_date_time = 1)  {
    xmlData <-
      data %>%
      dplyr::filter(idDateTime == id_date_time) %>%
      .$codesLanguages

    if (xmlData %>% is.na()) {
      xml_df <-
        data_frame(idDateTime = id_date_time)
    } else {
      xmlData <-
        xmlData %>%
        str_split('\\,') %>%
        flatten_chr()

      xml_df <-
        data_frame(idLanguage = xmlData) %>%
        mutate(idDateTime = id_date_time,
               idItemLanguage = 1:n()) %>%
        dplyr::select(idDateTime, idItemLanguage, everything()) %>%
        suppressWarnings()
    }

    return(xml_df)

  }

#' Parses Cloud Vision Languages
#'
#' @param gdelt_data
#' @param filter_na
#' @param return_wide
#'
#' @return
#' @export
#'
#' @examples
parse_cv_languages <- function(gdelt_data,
                               filter_na = T,
                               return_wide = F) {
  parse_language_types_safe <-
    failwith(NULL, parse_language_types)

  all_data <-
    gdelt_data$idDateTime %>%
    purrr::map(function(x) {
      parse_language_types_safe(data = gdelt_data, id_date_time = x)
    }) %>%
    purrr::compact %>%
    bind_rows %>%
    suppressWarnings()

  if (filter_na == T) {
    all_data <-
      all_data %>%
      dplyr::filter(!idItemLanguage %>% is.na)
  }
  return(all_data)
}
