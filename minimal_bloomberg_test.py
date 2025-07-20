#!/usr/bin/env python3
"""
Minimal Bloomberg API Test
Just test the basic connection
"""

def test_bloomberg_minimal():
    """Minimal Bloomberg test"""
    try:
        import blpapi
        print("✅ blpapi imported")
        
        sessionOptions = blpapi.SessionOptions()
        session = blpapi.Session(sessionOptions)
        
        print("🔄 Starting Bloomberg session...")
        if session.start():
            print("✅ Bloomberg session started!")
            
            print("🔄 Opening reference data service...")
            if session.openService("//blp/refdata"):
                print("✅ Reference data service opened!")
                
                # Test simple request
                service = session.getService("//blp/refdata")
                request = service.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue("VIX Index")
                request.getElement("fields").appendValue("PX_LAST")
                
                print("🔄 Sending test request for VIX Index...")
                session.sendRequest(request)
                
                # Get response with better error handling
                print("🔄 Waiting for response...")
                response_received = False
                
                while not response_received:
                    event = session.nextEvent(10000)  # 10 second timeout
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        print("✅ Got response from Bloomberg!")
                        response_received = True
                        
                        for msg in event:
                            print(f"📄 Message: {msg}")
                            securityDataArray = msg.getElement("securityData")
                            
                            for i in range(securityDataArray.numValues()):
                                securityData = securityDataArray.getValue(i)
                                security = securityData.getElement("security").getValue()
                                print(f"🔍 Security: {security}")
                                
                                if securityData.hasElement("securityError"):
                                    error = securityData.getElement("securityError")
                                    print(f"❌ Security error: {error}")
                                elif securityData.hasElement("fieldData"):
                                    fieldData = securityData.getElement("fieldData")
                                    if fieldData.hasElement("PX_LAST"):
                                        vix_price = fieldData.getElement("PX_LAST").getValue()
                                        print(f"✅ Current VIX: {vix_price}")
                                    else:
                                        print("⚠️  PX_LAST field not found")
                                        print(f"Available fields: {[fieldData.getElement(j).name() for j in range(fieldData.numElements())]}")
                                else:
                                    print("⚠️  No field data found")
                    
                    elif event.eventType() == blpapi.Event.TIMEOUT:
                        print("⚠️  Request timed out")
                        response_received = True
                        
                    elif event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                        print("🔄 Partial response received, waiting for more...")
                        
                    else:
                        print(f"🔄 Event type: {event.eventType()}")
                
                # Try alternative VIX tickers
                print("\n🔄 Testing alternative VIX tickers...")
                test_tickers = ["VIX Index", "VIX3 Index", "CBOE VIX1 Index"]
                
                for ticker in test_tickers:
                    print(f"   Testing: {ticker}")
                    request2 = service.createRequest("ReferenceDataRequest")
                    request2.getElement("securities").appendValue(ticker)
                    request2.getElement("fields").appendValue("PX_LAST")
                    
                    session.sendRequest(request2)
                    event2 = session.nextEvent(5000)
                    
                    if event2.eventType() == blpapi.Event.RESPONSE:
                        for msg2 in event2:
                            secData = msg2.getElement("securityData").getValue(0)
                            if secData.hasElement("fieldData"):
                                fieldData2 = secData.getElement("fieldData")
                                if fieldData2.hasElement("PX_LAST"):
                                    price = fieldData2.getElement("PX_LAST").getValue()
                                    print(f"   ✅ {ticker}: {price}")
                                    break
                            elif secData.hasElement("securityError"):
                                print(f"   ❌ {ticker}: Security error")
                            else:
                                print(f"   ⚠️  {ticker}: No data")
                    else:
                        print(f"   ⚠️  {ticker}: No response")
            else:
                print("❌ Failed to open reference data service")
            
            session.stop()
        else:
            print("❌ Failed to start Bloomberg session")
            
    except ImportError:
        print("❌ blpapi not installed. Install with: pip install blpapi")
    except Exception as e:
        print(f"❌ Bloomberg test failed: {e}")

if __name__ == "__main__":
    print("🔧 MINIMAL BLOOMBERG TEST")
    print("=" * 30)
    test_bloomberg_minimal()
