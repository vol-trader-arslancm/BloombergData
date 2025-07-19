"""
Bloomberg SPY Holdings Fields Test - Step 1
Try alternative ETF holdings fields systematically
"""

import sys
import os
import pandas as pd
from datetime import datetime
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import blpapi
    from config.bloomberg_config import SPY_TICKER
except ImportError as e:
    print(f"IMPORT ERROR: {e}")
    SPY_TICKER = 'SPY US Equity'

class SPYFieldsTester:
    """Test different Bloomberg fields for SPY holdings"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
    
    def connect(self):
        """Connect to Bloomberg Terminal"""
        try:
            sessionOptions = blpapi.SessionOptions()
            self.session = blpapi.Session(sessionOptions)
            
            if not self.session.start():
                print("ERROR: Failed to start Bloomberg session")
                return False
            
            if not self.session.openService("//blp/refdata"):
                print("ERROR: Failed to open Bloomberg reference data service")
                return False
            
            self.refDataService = self.session.getService("//blp/refdata")
            print("SUCCESS: Connected to Bloomberg for SPY fields testing")
            return True
            
        except Exception as e:
            print(f"ERROR: Bloomberg connection failed: {e}")
            return False
    
    def test_alternative_etf_fields(self):
        """Test Step 1: Alternative ETF holdings fields"""
        print("\n" + "="*60)
        print("STEP 1: TESTING ALTERNATIVE ETF HOLDINGS FIELDS")
        print("="*60)
        
        # Comprehensive list of potential ETF holdings fields
        test_fields = [
            # Basic holdings fields
            'FUND_HOLDINGS',
            'FUND_HOLDINGS_TICKER_AND_EXCHANGE',
            'FUND_PORTFOLIO_HOLDINGS',
            'FUND_HOLDINGS_MARKET_VALUE',
            'FUND_HOLDINGS_PERCENT_OF_FUND',
            'FUND_HOLDINGS_SHARES',
            'FUND_HOLDINGS_MWEIGHT',
            'FUND_HOLDINGS_NAME',
            'FUND_HOLDINGS_DETAILS',
            
            # Top holdings variations
            'FUND_TOP_10_HOLDINGS',
            'FUND_TOP_5_HOLDINGS',
            'FUND_TOP_20_HOLDINGS',
            'FUND_TOP_HOLDINGS',
            
            # Portfolio specific
            'PORTFOLIO_HOLDINGS',
            'PORTFOLIO_TOP_HOLDINGS',
            'PORTFOLIO_SECURITIES',
            
            # Alternative naming
            'ETF_HOLDINGS',
            'ETF_PORTFOLIO',
            'HOLDINGS_LIST',
            'SECURITIES_HELD',
            
            # Fund specific variations
            'FUND_SECURITY_NAME',
            'FUND_SECURITY_TICKER',
            'FUND_SECURITY_WEIGHT',
            'FUND_CONSTITUENT_TICKER',
            'FUND_CONSTITUENT_WEIGHT'
        ]
        
        results = {}
        
        for field in test_fields:
            print(f"\nTesting field: {field}")
            result = self._test_single_field(field)
            results[field] = result
            
            if result['success']:
                print(f"  SUCCESS: {result['data_type']} - {result['description']}")
            else:
                print(f"  FAILED: {result['error']}")
        
        return results
    
    def _test_single_field(self, field_name):
        """Test a single Bloomberg field"""
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(SPY_TICKER)
            request.getElement("fields").appendValue(field_name)
            
            # Add date override for holdings
            try:
                overrides = request.getElement("overrides")
                override = overrides.appendElement()
                override.setElement("fieldId", "FUND_HOLDINGS_AS_OF_DT")
                override.setElement("value", datetime.now().strftime('%Y%m%d'))
            except:
                pass  # Some fields don't need overrides
            
            self.session.sendRequest(request)
            
            # Process response
            while True:
                event = self.session.nextEvent(3000)  # 3 second timeout
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        security = securityData.getValue(0)
                        
                        if security.hasElement("securityError"):
                            error = security.getElement("securityError")
                            return {
                                'success': False,
                                'error': error.getElement('message').getValue(),
                                'data_type': None,
                                'description': None,
                                'data': None
                            }
                        
                        fieldData = security.getElement("fieldData")
                        
                        if fieldData.hasElement(field_name):
                            element = fieldData.getElement(field_name)
                            
                            # Analyze the data type and content
                            if element.isArray():
                                array_size = element.numValues()
                                if array_size > 0:
                                    sample = element.getValue(0)
                                    return {
                                        'success': True,
                                        'error': None,
                                        'data_type': 'Array',
                                        'description': f'{array_size} items, sample: {str(sample)[:100]}',
                                        'data': self._extract_array_data(element, max_items=5)
                                    }
                                else:
                                    return {
                                        'success': True,
                                        'error': None,
                                        'data_type': 'Empty Array',
                                        'description': 'Array with 0 items',
                                        'data': []
                                    }
                            else:
                                value = element.getValue()
                                return {
                                    'success': True,
                                    'error': None,
                                    'data_type': 'Single Value',
                                    'description': f'Value: {str(value)[:100]}',
                                    'data': value
                                }
                        else:
                            return {
                                'success': False,
                                'error': 'Field not found in response',
                                'data_type': None,
                                'description': None,
                                'data': None
                            }
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    return {
                        'success': False,
                        'error': 'Request timeout',
                        'data_type': None,
                        'description': None,
                        'data': None
                    }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'data_type': None,
                'description': None,
                'data': None
            }
    
    def _extract_array_data(self, element, max_items=5):
        """Extract sample data from array element"""
        try:
            data = []
            items_to_extract = min(element.numValues(), max_items)
            
            for i in range(items_to_extract):
                item = element.getValue(i)
                
                # Try to parse structured data
                if hasattr(item, 'numElements'):
                    item_dict = {}
                    for j in range(item.numElements()):
                        try:
                            field = item.getElement(j)
                            field_name = field.name()
                            field_value = field.getValue()
                            item_dict[field_name] = field_value
                        except:
                            continue
                    data.append(item_dict)
                else:
                    data.append(str(item))
            
            return data
        except Exception as e:
            return [f"Error extracting data: {e}"]
    
    def save_test_results(self, results):
        """Save test results to file"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            output_dir = os.path.join(project_root, 'data', 'processed', 'spy_holdings')
            os.makedirs(output_dir, exist_ok=True)
            
            # Save detailed results
            results_file = os.path.join(output_dir, f'bloomberg_fields_test_{timestamp}.json')
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            print(f"\nSUCCESS: Test results saved to: {results_file}")
            
            # Create summary of successful fields
            successful_fields = {k: v for k, v in results.items() if v['success']}
            
            if successful_fields:
                print(f"\nSUMMARY: Found {len(successful_fields)} working fields:")
                for field, result in successful_fields.items():
                    print(f"  {field}: {result['data_type']} - {result['description']}")
            else:
                print("\nSUMMARY: No fields returned data")
            
            return results_file
            
        except Exception as e:
            print(f"ERROR: Failed to save test results: {e}")
            return None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("SUCCESS: Bloomberg session disconnected")

def main():
    """Main execution function"""
    print("="*70)
    print("BLOOMBERG SPY HOLDINGS FIELDS TEST")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Testing alternative ETF holdings fields for SPY...")
    
    tester = SPYFieldsTester()
    
    try:
        if not tester.connect():
            return False
        
        # Test alternative ETF fields
        results = tester.test_alternative_etf_fields()
        
        # Save results
        tester.save_test_results(results)
        
        print(f"\nField testing completed! Check the results file for details.")
        return True
        
    except Exception as e:
        print(f"ERROR: Error in main execution: {e}")
        return False
    
    finally:
        tester.disconnect()

if __name__ == "__main__":
    main()