package bio.guoda.preston.spark

import org.scalatest._

import scala.io.Source

class ProbabilisticSignature$Test extends FlatSpec
  with Matchers
  with ProbabilisticSignatures {

  def generateRecords5gram(records: List[String]): Seq[String] = {
    generateRecordsNgram(records, 5)
  }

  def generateRecords3gram(records: List[String]): Seq[String] = {
    generateRecordsNgram(records, 3)
  }


  def generateRecordsNgram(records: List[String], n: Integer = 3): Seq[String] = {
    records.mkString(" ").sliding(n).map(_.mkString("").toLowerCase).filter(_.matches("^[a-zA-Z\\d]*$")).toSeq.distinct
  }

  def subRecordsFromWords(records: List[String]): Seq[String] = {
    records.mkString(" ").split(nonAlphaNumeric).toSeq.distinct
  }

  "split words" should "be suitable" in {
    "AE8C214A-BE9A-4B0A-83BE-E5A911B080F1/756".split(nonAlphaNumeric) should be(List("AE8C214A-BE9A-4B0A-83BE-E5A911B080F1/756"))
    "50.12".split(nonAlphaNumeric) should be(List("50.12"))
  }


  "an inverted index" should "be generated from a record" in {
    val recordsAll = List(
      "ZZdd Rattus rattus 2011",
      "1234 Homo sapiens 2018",
      "4567 Homo sapiens 2019",
      "4567 Homo saliens 2019",
      "1234 Homo sapiens 2019").distinct

    val a = 1.5
    val b = 0.6

    val some = calculatePairSharedSigProb(recordsAll, a, b, subRecordsFromWords)

    some.foreach(r => {
      val probabilitySharedSignature = r._2.formatted("%.2f")
      println(s"\n---\np=[$probabilitySharedSignature]\n${r._1._1}\n${r._1._2}")
    })

    some.reverse.head._2 should be(0.71 +- 0.01)
  }

  "an inverted index 3-grams" should "be generated from a record" in {
    val recordsAll = List(
      "ZZdd Rattus rattus 2011",
      "1234 Homo sapiens 2018",
      "4567 Homo sapiens 2019",
      "4567 Homo saliens 2019",
      "1234 Homo sapiens 2019").distinct

    val a = 1.5
    val b = 0.6

    val some = calculatePairSharedSigProb(recordsAll, a, b, generateRecords3gram)

    some.foreach(r => {
      val probabilitySharedSignature = r._2.formatted("%.2f")
      println(s"\n---\np=[$probabilitySharedSignature]\n${r._1._1}\n${r._1._2}")
    })

    some.reverse.head._2 should be(0.97 +- 0.01)
  }

  "an inverted index words" should "be generated from a record from actual records" in {

    val source1 = Source.fromInputStream(getClass.getResourceAsStream("/gbif/occurrence.txt"))
    val source2 = Source.fromInputStream(getClass.getResourceAsStream("/idigbio/occurrence.txt"))
    val recordsAll = (source1.getLines() ++ source2.getLines()).toList
    val a = 1.5
    val b = 0.3


    val some = calculatePairSharedSigProb(recordsAll, a, b, subRecordsFromWords)

    some.foreach(r => {
      val probabilitySharedSignature = r._2.formatted("%.2f")
      println(s"\n---\np=[$probabilitySharedSignature]\n${r._1._1}\n${r._1._2}")
    })

    some.reverse.head._2 should be(0.99 +- 0.01)
  }

  "an inverted index 5-grams" should "be generated from a record from actual records" in {

    val source = Source.fromInputStream(getClass.getResourceAsStream("/gbif/occurrence.txt"))
    val recordsAll = source.getLines().toList
    val a = 1.5
    val b = 0.6


    val some = calculatePairSharedSigProb(recordsAll, a, b, generateRecords5gram)

    some.foreach(r => {
      val probabilitySharedSignature = r._2.formatted("%.2f")
      println(s"\n---\np=[$probabilitySharedSignature]\n${r._1._1}\n${r._1._2}")
    })

    some.reverse.head._2 should be(0.99 +- 0.01)
  }

  "an inverted index 3-grams" should "be generated from a record from actual records" in {

    val source = Source.fromInputStream(getClass.getResourceAsStream("/gbif/occurrence.txt"))
    val recordsAll = source.getLines().toList
    val a = 1.5
    val b = 0.6


    val some = calculatePairSharedSigProb(recordsAll, a, b, generateRecords3gram)

    some.foreach(r => {
      val probabilitySharedSignature = r._2.formatted("%.2f")
      println(s"\n---\np=[$probabilitySharedSignature]\n${r._1._1}\n${r._1._2}")
    })

    some.reverse.head._2 should be(0.99 +- 0.01)
  }

  "an inverted index using word subrecords" should "be generated from a record from two lines" in {

    val recordsAll = List("id,dwc:county,dwc:locality,idigbio:tribe,dwc:infraspecificEpithet,dwc:rightsHolder,dwc:lithostratigraphicTerms,idigbio:associatedFamily,inhs:location_Basin,dwc:earliestAgeOrLowestStage,inhs:superfamily,dwc:ownerInstitutionCode,dwc:taxonRank,dwc:bed,inhs:Live,dwc:country,dwc:verbatimDepth,dcterms:source,dwc:maximumElevationInMeters,dwc:waterBody,dwc:family,idigbio:associateCondition,inhs:Dead,idigbio:associateAuthor,inhs:Instars_Male,dwc:identificationReferences,dwc:rights,dwc:island,dwc:geodeticDatum,dcterms:bibliographicCitation,dwc:nomenclaturalCode,dcterms:type,dwc:identificationQualifier,idigbio:associateCommonName,dwc:earliestEpochOrLowestSeries,dwc:namePublishedIn,dwc:verbatimCoordinateSystem,inhs:Juv_females,dwc:coordinateUncertaintyInMeters,dwc:day,dwc:lifeStage,dwc:identificationRemarks,dwc:verbatimTaxonRank,dwc:latestPeriodOrHighestSystem,idigbio:determinationHistory,dwc:recordedBy,dwc:order,dcterms:references,dwc:islandGroup,dcterms:accessRights,dwc:group,dwc:dateIdentified,dwc:informationWithheld,dwc:scientificNameID,dwc:verbatimElevation,dcterms:rightsHolder,dwc:establishmentMeans,inhs:Total_females,dwc:maximumDepthInMeters,dwc:typeStatus,inhs:FormII_females,dwc:verbatimLatitude,dwc:occurrenceStatus,dwc:locationID,dwc:basisOfRecord,dwc:taxonRemarks,inhs:Vouchered,dwc:latestEpochOrHighestSeries,dwc:dynamicProperties,dwc:municipality,inhs:FormII_males,dwc:previousIdentifications,dwc:latestAgeOrHighestStage,dwc:vernacularName,dwc:fieldNotes,dwc:institutionCode,dwc:class,dwc:member,dwc:verbatimLongitude,dwc:minimumDepthInMeters,dwc:verbatimLocality,inhs:Juv_males,idigbio:associateNotes,dwc:phylum,dcterms:rights,symbiotaverbatimScientificName,dwc:relatedResourceID,dwc:minimumElevationInMeters,dwc:associatedTaxa,inhs:locationTrs,dwc:samplingProtocol,dwc:startDayOfYear,dwc:verbatimCoordinates,idigbio:etag,idigbio:uuid,dwc:georeferencedDate,dwc:habitat,inhs:Instars_Female,dwc:scientificNameAuthorship,dwc:occurrenceID,dwc:associatedMedia,dwc:dataGeneralizations,inhs:location_RiverMile,idigbio:createdBy,inhs:location_Stream,idigbio:associateDeterminedBy,dwc:earliestPeriodOrLowestSystem,dwc:subgenus,dwc:decimalLatitude,idigbio:hostFamily,dwc:georeferenceRemarks,dwc:occurrenceRemarks,dwc:preparations,dwc:identificationVerificationStatus,idigbio:subfamily,idigbio:preparationCount,inhs:Total_Males,dwc:eventRemarks,dwc:associatedReferences,idigbio:endangeredStatus,dwc:georeferenceSources,dwc:associatedSequences,dwc:formation,dwc:higherClassification,dwc:catalogNumber,dwc:higherGeography,dwc:individualCount,dwc:decimalLongitude,dwc:datasetName,dwc:month,dwc:georeferencedBy,dwc:eventTime,dwc:identificationQualifier,idigbio:associateRelationship,dwc:state,dwc:specificEpithet,dwc:countryCode,idigbio:associateIdentifier,dwc:kingdom,dwc:fieldNumber,dwc:coordinatePrecision,idigbio:recordIds,dcterms:language,dwc:stateProvince,inhs:Relic,dwc:eventDate,dwc:collectionID,idigbio:recordId,dwc:collectionCode,dwc:nameAccordingTo,idigbio:barcodeValue,dwc:georeferenceVerificationStatus,dwc:associatedOccurrences,inhs:Juv_undetermined,dwc:recordNumber,dwc:genus,dwc:continent,dwc:sex,dwc:identifiedBy,idigbio:version,http://symbiota.org/terms/verbatimScientificName,dwc:disposition,inhs:FormI_males,dwc:preparation,dwc:latestEraOrHighestErathem,idigbio:associatedRelationship,idigbio:dateModified,dwc:locationAccordingTo,dwc:institutionID,dwc:locationRemarks,dwc:reproductiveCondition,dwc:eventID,idigbio:subgenus,dwc:endDayOfYear,dwc:scientificName,dwc:nomenclaturalStatus,dwc:otherCatalogNumbers,dwc:verbatimEventDate,dcterms:reference,dwc:accessRights,dwc:earliestEraOrLowestErathem,dwc:georeferenceProtocol,dcterms:modified,dwc:footprintWKT,dwc:datasetID,dwc:year",
      "gbifID	abstract	accessRights	accrualMethod	accrualPeriodicity	accrualPolicy	alternative	audience	available	bibliographicCitation	conformsTo	contributor	coverage	created	creator	date	dateAccepted	dateCopyrighted	dateSubmitted	description	educationLevel	extent	format	hasFormat	hasPart	hasVersion	identifier	instructionalMethod	isFormatOf	isPartOf	isReferencedBy	isReplacedBy	isRequiredBy	isVersionOf	issued	language	license	mediator	medium	modified	provenance	publisher	references	relation	replaces	requires	rights	rightsHolder	source	spatial	subject	tableOfContents	temporal	title	type	valid	acceptedNameUsage	acceptedNameUsageID	associatedOccurrences	associatedReferences	associatedSequences	associatedTaxa	basisOfRecord	bed	behavior	catalogNumber	class	collectionCode	collectionID	continent	countryCode	county	dataGeneralizations	datasetID	datasetName	dateIdentified	day	decimalLatitude	decimalLongitude	disposition	dynamicProperties	earliestAgeOrLowestStage	earliestEonOrLowestEonothem	earliestEpochOrLowestSeries	earliestEraOrLowestErathem	earliestPeriodOrLowestSystem	endDayOfYear	establishmentMeans	eventDate	eventID	eventRemarks	eventTime	family	fieldNotes	fieldNumber	footprintSRS	footprintSpatialFit	footprintWKT	formation	genus	geologicalContextID	georeferencedDate	georeferenceProtocol	georeferenceRemarks	georeferenceSources	georeferenceVerificationStatus	georeferencedBy	group	habitat	higherClassification	higherGeography	higherGeographyID	highestBiostratigraphicZone	identificationID	identificationQualifier	identificationReferences	identificationRemarks	identificationVerificationStatus	identifiedBy	individualCount	individualID	informationWithheld	infraspecificEpithet	institutionCode	institutionID	island	islandGroup	kingdom	latestAgeOrHighestStage	latestEonOrHighestEonothem	latestEpochOrHighestSeries	latestEraOrHighestErathem	latestPeriodOrHighestSystem	lifeStage	lithostratigraphicTerms	locality	locationAccordingTo	locationID	locationRemarks	lowestBiostratigraphicZone	materialSampleID	maximumDistanceAboveSurfaceInMeters	member	minimumDistanceAboveSurfaceInMeters	month	municipality	nameAccordingTo	nameAccordingToID	namePublishedIn	namePublishedInID	namePublishedInYear	nomenclaturalCode	nomenclaturalStatus	occurrenceID	occurrenceRemarks	occurrenceStatus	order	originalNameUsage	originalNameUsageID	otherCatalogNumbers	ownerInstitutionCode	parentNameUsage	parentNameUsageID	phylum	pointRadiusSpatialFit	preparations	previousIdentifications	recordNumber	recordedBy	reproductiveCondition	samplingEffort	samplingProtocol	scientificName	scientificNameID	sex	specificEpithet	startDayOfYear	stateProvince	subgenus	taxonConceptID	taxonID	taxonRank	taxonRemarks	taxonomicStatus	typeStatus	verbatimCoordinateSystem	verbatimDepth	verbatimElevation	verbatimEventDate	verbatimLocality	verbatimSRS	verbatimTaxonRank	vernacularName	waterBody	year	datasetKey	publishingCountry	lastInterpreted	coordinateAccuracy	elevation	elevationAccuracy	depth	depthAccuracy	distanceAboveSurface	distanceAboveSurfaceAccuracy	issue	mediaType	hasCoordinate	hasGeospatialIssues	taxonKey	kingdomKey	phylumKey	classKey	orderKey	familyKey	genusKey	subgenusKey	speciesKey	species	genericName	typifiedName	protocol	lastParsed	lastCrawled")

    val a = 1.5
    val b = 0.6

    val some = calculatePairSharedSigProb(recordsAll, a, b, subRecordsFromWords)

    some.foreach(r => {
      val probabilitySharedSignature = r._2.formatted("%.2f")
      println(s"\n---\np=[$probabilitySharedSignature]\n${r._1._1}\n${r._1._2}")
    })

    some.reverse.headOption should be(None)
  }

  "exclude subrecords" should "remove sub records that have other sub records as super records" in {
    val someSubRecords = List("one two", "one", "two")
    val independentSubRecords = excludeSubSubRecords(someSubRecords)

    "one two".split(nonAlphaNumeric) should contain("one")
    independentSubRecords should be(List("one two"))
  }


}
