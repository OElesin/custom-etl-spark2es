package org.custom.etl.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import org.joda.time.Days
import org.joda.time.DateTimeZone

object CustomUDFs {
  
  /*
   * @author: Olalekan Elesin
   * @description: This class contains SparkSQL UDFs for data extraction
   * @version: v20160819_snapshot01
   */
  
  private val DEFAULT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S")
  private val DERIVED_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S")
  private val YEAR_FORMAT = DateTimeFormat.forPattern("yyyy")
  private val EXTRACTED_FIELEDS = Seq("age", "geo_pol_zone", "hour_of_payment", "hour_of_signup", "hour_of_transaction", "days_to_convert", "days_to_apply")
  private val UNKNOWN: String = "unknown"
  private val DATE_FIELDS = Seq("signup_date", "application_date", "transaction_date", "payment_date")
  
  def getUserAge(dateOfBirth: Any) = {
    val currentYear = DateTime.now().getYear
    val yearOfBirth = DateTime.parse(dateOfBirth.toString(), DEFAULT_DATE_FORMAT).getYear
    currentYear - yearOfBirth
  }
  
  def toDate(rawDate: Any) = {
    val date = DateTime.parse(rawDate.toString(), DEFAULT_DATE_FORMAT)
    date.toDateTime(DateTimeZone.UTC).toString()
  }
  
  def getDaysDelta(upperBoundDate: Any, lowerBoundDate: Any) =  {
    val upperBound = DateTime.parse(upperBoundDate.toString(), DEFAULT_DATE_FORMAT)
    val lowerBound = DateTime.parse(upperBoundDate.toString(), DEFAULT_DATE_FORMAT)
    val difference = Days.daysBetween(upperBound, lowerBound).getDays
    difference
  }
  
  def getMobileNetwork(phoneNumber: String) {
    val networkProviders = Map(
     "MTN_Nigeria" -> Seq("0703", "0706", "0803", "0806", "0816", "0903"),
     "Etisalat_Nigeria" -> Seq("0809", "0817", "0818", "0809", "0909", "0908"),
     "Globalcom" -> Seq("0705", "0805", "0807", "0811", "0815", "0905"),
     "Airtel_Nigeria" -> Seq("0701", "0708", "0802", "0808", "0812", "0902")
    )
  }
  
  def getMobileNumberPrefix() {
    
  }
  
  def assignHourofDay(date: Any) = {
     val timeLabels = Map("morning" -> (7 to 12), "lunch" -> (12 to 14), "afternoon" -> (14 to 18), "evening" -> (18 to 24), "night" -> (0 to 7))
     val timestamp = DateTime.parse(date.toString(), DEFAULT_DATE_FORMAT)
     val hourOfDay = timestamp.getHourOfDay
     val hello = for( (k, v) <- timeLabels; if(v.contains(hourOfDay)) )  yield k
     val getTimeLable = if(!hello.isEmpty) hello.head else UNKNOWN
     getTimeLable
   }
  
  def assignMonthOfYear() {
    
  }
  
  def assignGeoPolZone(location: Any) = {
    val geoZones = Map(
      "NorthCentral" -> Seq("abuja", "benue", "kogi", "kwara", "nassarawa", "niger", "plateau"),
      "SouthWest" -> Seq("ekiti", "lagos", "ogun", "osun", "oyo"),
      "NorthEast" -> Seq("adamawa", "bauchi", "borno", "gombe", "taraba", "yobe"),
      "NorthWest" -> Seq("jigawa", "kaduna", "kano", "katsina", "kebbi", "sokoto", "zamfara"),
      "SouthEast" -> Seq("abia", "anambra", "ebonyi", "enugu", "imo"),
      "SouthSouth" -> Seq("akwa-ibom", "bayelsa", "cross river", "delta", "edo", "rivers")
    )
    val temporaryPayload = for( (zone, states) <- geoZones; if(states.contains(location.toString.toLowerCase())) ) yield zone
    val geoPolZone = if(!temporaryPayload.isEmpty) temporaryPayload.head else UNKNOWN 
    geoPolZone
  }
  
  def extractValues(userData: Map[String, Any]) = {
    val age = if(userData.isDefinedAt("dateofbirth")) getUserAge(userData("dateofbirth")) else 0
    val geoPolZone = if(userData.isDefinedAt("location")) assignGeoPolZone(userData("location")) else null
    val paymentHour = if(userData.isDefinedAt("PaymentDate")) assignHourofDay(userData("PaymentDate")) else null
    val signupHour = if(userData.isDefinedAt("SignUpDate")) assignHourofDay(userData("SignUpDate")) else null
    val transactionHour = if(userData.isDefinedAt("transactiondate")) assignHourofDay(userData("transactiondate")) else null
    val daysToConvert = if(userData.isDefinedAt("SignUpDate") && userData.isDefinedAt("PaymentDate") ) getDaysDelta(userData("PaymentDate"), userData.isDefinedAt("SignUpDate")) else null
    val daysToApply = if(userData.isDefinedAt("applicationdate") && userData.isDefinedAt("SignUpDate") ) getDaysDelta(userData("applicationdate"), userData.isDefinedAt("SignUpDate")) else null
    
    val signUpDate = if(userData.isDefinedAt("SignUpDate")) toDate(userData("SignUpDate")) else 0
    val applicationDate = if(userData.isDefinedAt("SignUpDate")) toDate(userData("applicationdate")) else 0
    val transactionDate = if(userData.isDefinedAt("transactiondate")) toDate(userData("transactiondate")) else 0
    val paymentDate = if(userData.isDefinedAt("PaymentDate")) toDate(userData("PaymentDate")) else 0
    
    val dateFields = DATE_FIELDS.zip(Seq(signUpDate, applicationDate, transactionDate, paymentDate)).toMap
    val extractedObjects = Seq(age, geoPolZone, paymentHour, signupHour, transactionHour, daysToConvert, daysToApply)
    val fieldsZippedWithValues = EXTRACTED_FIELEDS.zip(extractedObjects).toMap
    val prunned = userData.-("PaymentDate", "SignUpDate", "transactiondate", "applicationdate")
    prunned ++ fieldsZippedWithValues ++ dateFields
  }
}