package com.interana.eventsim

import scala.collection.mutable
import com.interana.eventsim.buildin.RandomStringGenerator

class Cart extends Extractable {
  def getNamespace() = {
    "Cart"
  }
  
  def getId() = {
    RandomStringGenerator.randomThing
  }
  
  val attributes = new mutable.HashMap[String, Any]()
  
  var items:Option[mutable.ArrayBuffer[CartItem]] = None
    
  def getCSVMap() = {
    Map("total" -> getTotal(), "size" -> getSize())
  }
  
  def clearCart() = {
    items = None
  }
  
  def addItem(product:Product, qty:Integer) = {
    if(!items.isDefined) {
      items = Some(mutable.ArrayBuffer[CartItem]())
    }
    items.get += new CartItem(product, qty)
  }
  
  def getTotal() = {
    var total = 0.0
    
    if(items.isDefined) {
      items.get.foreach { ci =>
        if(ci.product.price.isDefined) {
          total += ci.product.price.get
        }
      }
    }
    
    total
  }
  
  def getSize() = {
    var cartSize = 0
    
    if(items.isDefined) {
      items.get.foreach { ci =>
        cartSize += 1
      }
    }
    
    cartSize
  }
  
  def getQty() = {
    var qty = 0
    
    if(items.isDefined) {
      items.get.foreach { ci =>
        qty += ci.qty
      }
    }
    
    qty
  }
  
  def getAttributeMap() = {
    mutable.HashMap("id" -> getId(), "itemCount" -> getSize(), "totalQty" -> getQty(), "total" -> getTotal())
  }
  
  def getCheckoutProps() = {
    Some(Map("checkout_items" -> getSize(), "checkout_qty" -> getQty(), "checkout_total" -> getTotal()))
  }
  
  def getCartProps() = {    
    Some(Map("cart_items" -> getSize(), "cart_qty" -> getQty(), "cart_total" -> getTotal()))
  }
}

object Cart extends Actor {
  def getNamespace() = {
    "Cart"
  }
  
  def getCSVHeaders() = {
    List("total", "size")
  }
}