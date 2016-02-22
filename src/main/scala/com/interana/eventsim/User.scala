package com.interana.eventsim

import java.io.{OutputStream, Serializable, FileWriter}
import java.time.{ZoneOffset, LocalDateTime}
import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory}
import com.interana.eventsim.config.ConfigFromFile

import scala.collection.mutable
import scala.util.parsing.json.JSONObject
import scala.io.Source

class User(val alpha: Double,
           val beta: Double,
           val startTime: LocalDateTime,
           val initialSessionStates: scala.collection.Map[(String,String),WeightedRandomThingGenerator[State]],
           val auth: String,
           val props: Map[String,Any],
           var device: scala.collection.immutable.Map[String,Any],
           val initialLevel: String,
           val stream: OutputStream,
           val csvstream: OutputStream,
           val imstream: OutputStream
          ) extends Serializable with Ordered[User] {

  val userId = Counters.nextUserId
  var session = new Session(
    Some(Session.pickFirstTimeStamp(startTime, alpha, beta)),
      alpha, beta, initialSessionStates, auth, initialLevel)
  
  var lastEventTime = this.session.nextEventTimeStamp
  var prevSessionId = this.session.sessionId

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
      if (TimeUtilities.rng.nextDouble() < prAttrition ||
          session.currentState.auth == ConfigFromFile.churnedState.getOrElse("")) {
        session.nextEventTimeStamp = None
        // TODO: mark as churned
      }
      else {
        session = session.nextSession
      }
    }
  }

  private val EMPTY_MAP = Map()

  def eventString = {
    val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    var m = device.+(
      "ts" -> sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
      "userId" -> (if (showUserDetails) userId else ""),
      "sessionId" -> session.sessionId,
      "page" -> session.currentState.page,
      "auth" -> session.currentState.auth,
      "method" -> session.currentState.method,
      "status" -> session.currentState.status,
      "itemInSession" -> session.itemInSession
    )

    if (showUserDetails)
      m ++= props

    /* most of the event generator code is pretty generic, but this is hard-coded
     * for a fake music web site
     */
    if (session.currentState.page=="NextSong")
      m += (
        "artist" -> session.currentSong.get._2,
        "song" -> session.currentSong.get._3,
        "length" -> session.currentSong.get._4
        )

    val j = new JSONObject(m)
    j.toString()
  }


  val writer = User.jsonFactory.createGenerator(stream, JsonEncoding.UTF8)
  val imwriter = User.jsonFactory.createGenerator(imstream, JsonEncoding.UTF8)

  def writeEvent() = {
    // use Jackson streaming to maximize efficiency
    // (earlier versions used Scala's std JSON generators, but they were slow)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val sdfSimple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
    writer.writeStartObject()
    writer.writeStringField("eventTime", sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
    writer.writeStringField("actors.CUSTOMER.CUSTOMER_ID", if (showUserDetails) userId.toString else "")
    writer.writeNumberField("actors.CUSTOMER.SESSION_ID", session.sessionId)
    writer.writeStringField("eventType", session.currentState.page)
    writer.writeStringField("labels.AUTH", session.currentState.auth)
    writer.writeStringField("labels.HTTP_METHOD", session.currentState.method)
    writer.writeNumberField("labels.HTTP_STATUS", session.currentState.status)
    writer.writeStringField("labels.USER_LEVEL", session.currentState.level)
    writer.writeNumberField("labels.ITEM_IN_SESSION", session.itemInSession)
    if (showUserDetails) {
      props.foreach((p: (String, Any)) => {
        p._2 match {
          case _: Long => writer.writeNumberField(p._1, p._2.asInstanceOf[Long])
          case _: Int => writer.writeNumberField(p._1, p._2.asInstanceOf[Int])
          case _: Double => writer.writeNumberField(p._1, p._2.asInstanceOf[Double])
          case _: Float => writer.writeNumberField(p._1, p._2.asInstanceOf[Float])
          case _: String => writer.writeStringField(p._1, p._2.asInstanceOf[String])
          case _: Date => writer.writeStringField(p._1, sdf.format(p._2).toString)
        }})
      if (Main.tag.isDefined)
        writer.writeStringField("actors.CUSTOMER.TAG", Main.tag.get)
    }
    if (session.currentState.page=="NextSong") {
      writer.writeStringField("artist", session.currentSong.get._2)
      writer.writeStringField("song",  session.currentSong.get._3)
      writer.writeNumberField("length", session.currentSong.get._4)
    }
    writer.writeEndObject()
    writer.writeRaw('\n')
    writer.flush()
    
    var propArray:Array[String] = Array()
    if (showUserDetails) {
      props.foreach((p: (String, Any)) => {
        var propItem:Any = None
        p._2 match {
          case _: Long => propItem = p._2.asInstanceOf[Long]
          case _: Int => propItem = p._2.asInstanceOf[Int]
          case _: Double => propItem = p._2.asInstanceOf[Double]
          case _: Float => propItem = p._2.asInstanceOf[Float]
          case _: String => propItem = p._2.asInstanceOf[String]
          case _: Date => propItem = sdfSimple.format(p._2)
        }
        propArray = propArray :+ propItem.toString
      })
    }
    val propString = propArray.mkString("\t")
    val csvString:String = "%s\t%s\t%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n".format(
        sdfSimple.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
        session.currentState.page,
        session.sessionId,
        sdfSimple.format(new Date(this.lastEventTime.get.toInstant(ZoneOffset.UTC).toEpochMilli)),
        session.previousState.page,
        this.prevSessionId,
        if (showUserDetails) userId.toString else "",
        if (Main.tag.isDefined) Main.tag.get else "None",
        session.currentState.auth,
        session.currentState.method,
        session.currentState.status,
        session.currentState.level,
        session.itemInSession,
        propString
    )
    csvstream.write(csvString.getBytes)
    
    
    // Writing IM File
    imwriter.writeStartObject()
    imwriter.writeStringField("event type", session.currentState.page)
    imwriter.writeStringField("event time", sdf.format(new Date(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli)))
    imwriter.writeObjectFieldStart("actors")
    imwriter.writeStringField("customer", if (showUserDetails) userId.toString else "")
    imwriter.writeNumberField("session", session.sessionId)
    imwriter.writeEndObject()
    
    imwriter.writeObjectFieldStart("labels")
    if (showUserDetails) {
      props.foreach((p: (String, Any)) => {
        val key = p._1.split("""\.""").last.toLowerCase()
        p._2 match {
          case _: String => imwriter.writeStringField(key, p._2.asInstanceOf[String])
          case _: Date => imwriter.writeStringField(key, sdf.format(p._2).toString)
          case _ => 
        }})
      if (Main.tag.isDefined)
        imwriter.writeStringField("tag", Main.tag.get)
    }
    imwriter.writeStringField("auth", session.currentState.auth)
    imwriter.writeStringField("http_method", session.currentState.method)
    imwriter.writeStringField("http_status", session.currentState.status.toString)
    imwriter.writeStringField("user_level", session.currentState.level)
    imwriter.writeEndObject()
    
    imwriter.writeObjectFieldStart("values")
    imwriter.writeNumberField("item_in_session", session.itemInSession)
    if (showUserDetails) {
      props.foreach((p: (String, Any)) => {
        val key = p._1.split("""\.""").last.toLowerCase()
        p._2 match {
          case _: Long => imwriter.writeNumberField(key, p._2.asInstanceOf[Long])
          case _: Int => imwriter.writeNumberField(key, p._2.asInstanceOf[Int])
          case _: Double => imwriter.writeNumberField(key, p._2.asInstanceOf[Double])
          case _: Float => imwriter.writeNumberField(key, p._2.asInstanceOf[Float])
          case _ =>
        }})
      if (Main.tag.isDefined)
        imwriter.writeStringField("tag", Main.tag.get)
    }
    imwriter.writeEndObject()
    
    imwriter.writeEndObject()
    imwriter.writeRaw('\n')
    imwriter.flush()
    
    
    
    this.lastEventTime = session.nextEventTimeStamp
    this.prevSessionId = session.sessionId
  }

  def tsToString(ts: LocalDateTime) = ts.toString()

  def nextEventTimeStampString =
    tsToString(this.session.nextEventTimeStamp.get)

  def mkString = props.+(
    "alpha" -> alpha,
    "beta" -> beta,
    "startTime" -> tsToString(startTime),
    "initialSessionStates" -> initialSessionStates,
    "nextEventTimeStamp" -> tsToString(session.nextEventTimeStamp.get) ,
    "sessionId" -> session.sessionId ,
    "userId" -> userId ,
    "currentState" -> session.currentState)
}

object User {
  protected val jsonFactory = new JsonFactory()
  jsonFactory.setRootValueSeparator("")
}