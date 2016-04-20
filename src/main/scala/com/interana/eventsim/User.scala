package com.interana.eventsim

import java.io.{OutputStream, Serializable, FileWriter}
import java.time.{ZoneOffset, LocalDateTime}
import java.text.SimpleDateFormat
import java.util.Date
import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory}
import com.interana.eventsim.config.ConfigFromFile
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.parsing.json.JSONObject
import scala.io.Source
import com.interana.eventsim.buildin.{RandomFirstNameGenerator, RandomLastNameGenerator, RandomStringGenerator, RandomEmailGenerator, DeviceProperties, UserProperties}
import com.interana.eventsim.messages._
import com.interana.eventsim.devices._
import com.interana.eventsim.channels._
import com.interana.eventsim.channels.interaction._
import com.interana.eventsim.channels.marketing._
import com.interana.eventsim.EventType
import com.interana.eventsim.channels.Phone

// TODO: Refactor User class to make it easily understandable. User has multiple devices, each device generates multiple sessions.

class User(val startTime: LocalDateTime,
           val writers: Map[String,OutputStream]
          ) extends Serializable with Ordered[User] with Extractable {
  def getNamespace() = {
    "User"
  }
  
//  println(ConfigFromFile.eventTypes)
  /*
   * Configurations
   */
  
  private val sqrtE = Math.exp(0.5)
  def logNormalRandomValue = Math.exp(TimeUtilities.rng.nextGaussian()) / sqrtE
  
  val alpha = ConfigFromFile.alpha * logNormalRandomValue
  val beta = ConfigFromFile.beta * logNormalRandomValue
  val initialSessionStates = ConfigFromFile.initialStates
  var auth = ConfigFromFile.authGenerator.randomThing
  var userAttributes = UserProperties.customProps(ConfigFromFile.individualAttributeSetGenerator)
  val device = DeviceProperties.randomProps
  var level = ConfigFromFile.levelGenerator.randomThing
  
  
  /*
   * TODO: What needs to be defined
   * 	user id
   * 	basic profile (first name, last name, address, demographic information)
   * 	user-level event transitions
   * 	user status (logged in, logged out)
   * 	list of devices
   * 	list of sessions
   * 	channel preferences
   * 	coupon queue
   * 	loyalty information
   * 	cart, cartItem, orders
   * 	loyalty
   */
  
  // user id
  val userId = Counters.nextUserId
  
  // basic profile
  userAttributes.put("id", userId.toString())
  userAttributes.put("firstName", RandomFirstNameGenerator.randomThing._1)
  userAttributes.put("lastName", RandomLastNameGenerator.randomThing)
  userAttributes.put("email", RandomEmailGenerator.randomThing)
  // refer to userAttribute Map for additional attributes
  
  var exposedAttributes = Set[String]()
  
  // user-level event transitions
  val transitions:HashMap[(EventType, EventType),Any] = new HashMap[(EventType, EventType),Any]()
  
  // list of devices
  val devices:HashMap[String,Device] = generateDevices(userAttributes("age").asInstanceOf[String])
  
  // channels
  val channels:HashMap[String,Channel] = generateChannels(devices)
  
  // list of sessions
  val sessions:HashMap[String,Session] = new HashMap[String,Session]()
  
  // channel preferences
  val channelPreferences = User.getChannelPreference(userAttributes("age").asInstanceOf[String], channels)
  
  // coupon stack
  val coupons:ArrayBuffer[Coupon] = new ArrayBuffer[Coupon]()
  
  // cart
  val cart = new Cart()
  
  // loyalty
  val loyaltyId = RandomStringGenerator.randomAlphanumericString(20)
  val loyaltyPoints = 0.0
  
  // entry points
  val entryPoints = ConfigFromFile.entryPoints

  
  
  var session = Session.createSession(this)
  
//  val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
  
  val eventsWithProductData:List[String] = List("Product Searched", "Return in Store", "Add To Cart", "Bar Code Scanned", "Product Compared", "Product Rated", "Product Recommended", "Product Reviewed", "Product Shared", "Product Viewed") 
  val eventsWithCustomerData:List[String] = List("Set Customer Profile")
  
  var lastEventTime = this.session.nextEventTimeStamp
  var prevSessionId = this.session.sessionId
  var prevProducts = ArrayBuffer[Product]()
  var lastProduct:Option[Product] = None
  
  val attributes = new HashMap[String, Any]()
  
  def getAttributeMap() = {
    getExposedAttributeMap
  }
  
  def getId() = {
    userId.toString()
  }
  
  def getCSVMap() = {
    userAttributes.toMap
  }
  

  override def compare(that: User) =
    (that.session.nextEventTimeStamp, this.session.nextEventTimeStamp) match {
      case (None, None) => 0
      case (_: Some[LocalDateTime], None) => -1
      case (None, _: Some[LocalDateTime]) => 1
      case (thatValue: Some[LocalDateTime], thisValue: Some[LocalDateTime]) =>
        thatValue.get.compareTo(thisValue.get)
    }

  def nextEvent(): Unit = nextEvent(0.0)

  def nextEvent(prAttrition: Double) = {
    session.incrementEvent()
    if (session.done) {
      val lastEventName = if(session.previousEvent.isDefined) session.previousEvent.get.eventType.name else None
//      if (session.previousEvent.isDefined) this.writeEvent()
      if (TimeUtilities.rng.nextDouble() < prAttrition ||
          ConfigFromFile.churnedStates.contains(lastEventName)) {
        session.nextEventTimeStamp = None
        // TODO: mark as churned
      }
      else {
        session = session.nextSession
      }
    }
  }

  private val EMPTY_MAP = Map()

  val writer = User.jsonFactory.createGenerator(writers("dp"), JsonEncoding.UTF8)
  val imwriter = User.jsonFactory.createGenerator(writers("im"), JsonEncoding.UTF8)
  val csvstream = writers("csv")
  
  def getDeviceForChannel(channel:Channel) = {
    val randomDeviceSelector = new WeightedRandomThingGenerator[Device]()
    devices.foreach(d => 
      if(d._2.supportedChannels.contains(channel.name)) {
        randomDeviceSelector.add(d._2, 1)
      }
    )
    
    randomDeviceSelector.randomThing
  }
  
  def writeEvent() = {    
    val eventType = session.currentEvent.eventType
        
    session.currentEvent.writeEvent(writer, csvstream, imwriter)

    this.lastEventTime = session.nextEventTimeStamp
    this.prevSessionId = session.sessionId
  }

  def tsToString(ts: LocalDateTime) = ts.toString()

  def nextEventTimeStampString =
    tsToString(this.session.nextEventTimeStamp.get)
  
  def pickDevice() = {
    var device:Option[Device] = None
    
    while(device.isEmpty) {
      var channelName = channelPreferences.randomThing
      if(channels.keySet.contains(channelName)) {
        device = Some(getDeviceForChannel(channels.get(channelName).get))
      }
    }
    
    device
  }
  
  // generate channels
  def generateChannels(devices:HashMap[String,Device]) = {
    val channels = HashMap[String,Channel]()
    
    devices.foreach{d => 
      d._2.supportedChannels.foreach { c => 
        if(!channels.keySet.contains(c)) {
          c match {
            case "Phone" => channels.put("Phone", new Phone())
            case "Email" => channels.put("Email", new Email())
            case "Social" => channels.put("Social", new Social())
            case "MobileApp" => channels.put("MobileApp", new MobileApp())
            case "Mail" => channels.put("Mail", new Mail())
            case "Website" => channels.put("Website", new Website())
            case "SMS" => channels.put("SMS", new SMS())
            case "DisplayAd" => channels.put("DisplayAd", new DisplayAd())
            case "SearchEngine" => channels.put("SearchEngine", new SearchEngine())
            case _ => println("unsupported channel", c)
          }
        }
      }
    }
    
    channels
  }
  
  
  // generate devices according to some 
  def generateDevices(age:String) = {
    val devices:HashMap[String,Device] = new HashMap[String,Device]()
    
    val distribution:(Double, Double, Double, Double) = age match {
      case "13-17" => (30.0, 15.0, 70.0, 5.0)
      case "18-24" => (30.0, 35.0, 90.0, 5.0)
      case "25-34" => (30.0, 35.0, 90.0, 10.0)
      case "35-44" => (30.0, 35.0, 80.0, 10.0)
      case "45-54" => (20.0, 25.0, 70.0, 10.0)
      case "55-64" => (15.0, 15.0, 50.0, 30.0)
      case "65-75" => (10.0, 15.0, 30.0, 25.0)
      case "+75" => (5.0, 10.0, 25.0, 10.0)
      case _ => (0.0, 0.0, 0.0, 0.0)
    }
    
    var generator = new WeightedRandomThingGenerator[String]()
    generator.add("desktop", distribution._1.toInt)
    generator.add("laptop", distribution._2.toInt)
    generator.add("mobilephone", distribution._3.toInt)
    generator.add("tablet", distribution._4.toInt)
    
    
    val numDevice = TimeUtilities.rng.nextInt(3)+1
    
    for(i <- Iterator.range(0, numDevice)) {
      val deviceType = generator.randomThing
      devices += (deviceType -> createDevice(deviceType))
    }
    
    devices
  }
  
  def createDevice(deviceType:String) = {
    deviceType match {
      case "desktop" => new Desktop()
      case "laptop" => new Laptop()
      case "mobilephone" => new MobilePhone()
      case "tablet" => new Tablet()
    }
  }
  
  def getExposedAttributeMap = {
    val exposedAttributeMap = new HashMap[String, Any]()
    exposedAttributes.foreach{ a:String =>
      if(userAttributes.keySet.contains(a)) {
        exposedAttributeMap.put(a, userAttributes.get(a).get)
      }
    }
    exposedAttributeMap
  }
}

object User extends Actor {
  protected val jsonFactory = new JsonFactory()
  jsonFactory.setRootValueSeparator("")
  
  // get channel preference
  def getChannelPreference(age:String, channels:HashMap[String,Channel]):WeightedRandomThingGenerator[String] = {
    /*
     *  Channel preferences are from zendesk whitepaper
     *  https://www.zendesk.com/resources/a-guide-to-multi-channel-customer-support/
     */
    
    val channelPreference:(Double, Double, Double, Double, Double, Double) = age match {
      case "13-17" => (29.4, 42.3, 36.4, 31.9, 5.9, 20.0)
      case "18-24" => (29.4, 42.3, 36.4, 31.9, 5.9, 20.0)
      case "25-34" => (46.3, 44.1, 20.7, 17.2, 9.6, 20.0)
      case "35-44" => (46.3, 44.1, 20.7, 17.2, 9.6, 20.0)
      case "45-54" => (46.3, 44.1, 20.7, 17.2, 9.6, 20.0)
      case "55-64" => (59.6, 22.5, 3.5, 3.0, 13.4, 20.0)
      case "65-75" => (59.6, 22.5, 3.5, 3.0, 13.4, 20.0)
      case "+75" => (55.6, 6.6, 0.7, 0.3, 17.2, 20.0)
      case _ => (0.0, 0.0, 0.0, 0.0, 0.0, 100.0)
    }
    
    val generator = new WeightedRandomThingGenerator[String]()
    if(channels.keySet.contains("Website") && channelPreference._1 > 0.0) {
      generator.add("Website", (channelPreference._1*10).toInt)
    }
    if(channels.keySet.contains("Email") && channelPreference._2 > 0.0) {
      generator.add("Email", (channelPreference._2*10).toInt)
    }
    if(channels.keySet.contains("Social") && channelPreference._3 > 0.0) {
      generator.add("Social", (channelPreference._3*10).toInt)
    }
    if(channels.keySet.contains("MobileApp") && channelPreference._4 > 0.0) {
      generator.add("MobileApp", (channelPreference._4*10).toInt)
    }
    if(channels.keySet.contains("Mail") && channelPreference._5 > 0.0) {
      generator.add("Mail", (channelPreference._5*10).toInt)
    }
    if(channels.keySet.contains("Offline") && channelPreference._6 > 0.0) {
      generator.add("Offline", (channelPreference._6*10).toInt)
    }
    
    generator
  }
  
  /*
 	* Actor Methods
 	*/
  
  def getNamespace() = {
    "User"
  }
  
  def getCSVHeaders() = {
    List("id", "firstName", "lastName", "email", "gender", "race", "income", "age", "education", "employment", "marital status", "car", "interest", "activity")
  }
}