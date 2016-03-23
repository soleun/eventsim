package com.interana.eventsim

import scala.collection.mutable.ArrayBuffer

class Cart {
  var items:Option[ArrayBuffer[CartItem]] = None
  
  def clearCart() = {
    items = None
  }
  
  def addItem(product:Product, qty:Integer) = {
    if(!items.isDefined) {
      items = Some(ArrayBuffer[CartItem]())
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
  
  def getCheckoutProps() = {
    var cartSize = 0
    var cartTotal = 0.0
    if(items.isDefined) {
      cartSize = items.get.size
      cartTotal = getTotal()
    }
    
    Some(Map("checkout_items" -> cartSize, "checkout_total" -> cartTotal))
  }
  
  def getCartProps() = {
    var cartSize = 0
    var cartTotal = 0.0
    if(items.isDefined) {
      cartSize = items.get.size
      cartTotal = getTotal()
    }
    
    Some(Map("cart_items" -> cartSize, "cart_total" -> cartTotal))
  }
}