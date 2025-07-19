"""
Bloomberg SPY Membership Fields Test - Option 2
Try ETF membership/constituent fields (replicating SPY EQUITY MEMB terminal command)
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

class SPYMembershipTester:
    """Test ETF membership/constituent fields for SPY"""
    
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
            print("SUCCESS: Connected to Bloomberg for SPY membership testing")
            return True
            
        except Exception as e:
            print(f"ERROR: Bloomberg connection failed: {e}")
            return False
    
    def test_membership_fields(self):
        """Test ETF membership/constituent fields"""
        print("\n" + "="*60)
        print("OPTION 2: TESTING ETF MEMBERSHIP/CONSTITUENT FIELDS")
        print("="*60)
        
        # ETF membership/constituent fields (replicating MEMB terminal command)
        membership_fields = [
            # Index/ETF Members
            'INDX_MEMBERS',
            'INDX_MWEIGHT', 
            'INDX_CONSTITUENT',
            'FUND_MEMBERS',
            'ETF_MEMBERS',
            'ETF_CONSTITUENTS',
            
            # Constituent variations
            'CONSTITUENT_TICKERS',
            'CONSTITUENT_WEIGHTS',
            'CONSTITUENT_NAMES',
            'CONSTITUENT_SHARES',
            'CONSTITUENT_MKT_VAL',
            
            # Member variations
            'MEMBER_TICKERS',
            'MEMBER_WEIGHTS', 
            'MEMBER_NAMES',
            'MEMBERS_LIST',
            
            # Portfolio composition
            'PORTFOLIO_COMPOSITION',
            'PORTFOLIO_WEIGHTS',
            'PORTFOLIO_CONSTITUENTS',
            
            # ETF specific
            'ETF_PORTFOLIO_COMPOSITION',
            'ETF_UNDERLYING_ASSETS',
            'ETF_BASKET_COMPOSITION',
            
            # Alternative membership
            'FUND_COMPOSITION',
            'FUND_CONSTITUENTS',
            'SECURITY_CONSTITUENTS'
        ]
        
        results = {}
        
        for field in membership_fields:
            print(f"\nTesting membership field: {field}")
            result = self._test_single_field(field)
            results[field] = result
            
            if result['success']:
                print(f"  SUCCESS: {result['data_type']} - {result['description']}")
                
                # If we get array data with holdings, show more details
                if result['data_type'] == 'Array' and result['data']:
                    print(f"    Sample data: {result['data'][:2]}")  # Show first 2 items
            else:
                print(f"  FAILED: {result['error']}")
        
        return results
    
    def _test_single_field(self, field_name):
        """Test a single Bloomberg membership field"""
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(SPY_TICKER)
            request.getElement("fields").appendValue(field_name)
            
            self.session.sendRequest(request)
            
            # Process response
            while True:
                event = self.session.nextEvent(5000)  # 5 second timeout for membership data
                
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
                                    return {
                                        'success': True,
                                        'error': None,
                                        'data_type': 'Array',
                                        'description': f'{array_size} members/constituents',
                                        'data': self._extract_membership_data(element, max_items=10)
                                    }
                                else:
                                    return {
                                        'success': True,
                                        'error': None,
                                        'data_type': 'Empty Array',
                                        'description': 'Array with 0 members',
                                        'data': []
                                    }
                            else:
                                value = element.getValue()
                                return {
                                    'success': True,
                                    'error': None,
                                    'data_type': 'Single Value',
                                    'description': f'Value: {str(value)[:200]}',
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
    
    def _extract_membership_data(self, element, max_items=10):
        """Extract membership/constituent data from array element"""
        try:
            data = []
            items_to_extract = min(element.numValues(), max_items)
            
            for i in range(items_to_extract):
                item = element.getValue(i)
                
                # Try to parse structured membership data
                if hasattr(item, 'numElements'):
                    member_dict = {}
                    for j in range(item.numElements()):
                        try:
                            field = item.getElement(j)
                            field_name = field.name()
                            field_value = field.getValue()
                            member_dict[field_name] = field_value
                        except:
                            continue
                    
                    # Look for common membership fields
                    if member_dict:
                        data.append(member_dict)
                    else:
                        data.append(f"Structured item {i}: {str(item)[:100]}")
                else:
                    # Simple string/value
                    data.append(str(item))
            
            return data
        except Exception as e:
            return [f"Error extracting membership data: {e}"]
    
    def save_membership_results(self, results):
        """Save membership test results"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            output_dir = os.path.join(project_root, 'data', 'processed', 'spy_holdings')
            os.makedirs(output_dir, exist_ok=True)
            
            # Save detailed results
            results_file = os.path.join(output_dir, f'bloomberg_membership_test_{timestamp}.json')
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            print(f"\nSUCCESS: Membership test results saved to: {results_file}")
            
            # Create summary of successful fields
            successful_fields = {k: v for k, v in results.items() if v['success']}
            
            if successful_fields:
                print(f"\nSUMMARY: Found {len(successful_fields)} working membership fields:")
                
                # Show the most promising results
                for field, result in successful_fields.items():
                    print(f"\n  {field}:")
                    print(f"    Type: {result['data_type']}")
                    print(f"    Description: {result['description']}")
                    
                    # Show sample data for arrays
                    if result['data_type'] == 'Array' and result['data']:
                        print(f"    Sample: {result['data'][0] if result['data'] else 'No sample'}")
                        
                print(f"\nMOST PROMISING FIELDS for SPY holdings:")
                array_fields = [(k, v) for k, v in successful_fields.items() 
                              if v['data_type'] == 'Array' and v['data']]
                
                if array_fields:
                    for field, result in array_fields:
                        print(f"  -> {field}: {result['description']}")
                else:
                    print("  No array fields with member data found")
                    
            else:
                print("\nSUMMARY: No membership fields returned data")
                print("This suggests SPY membership data may require:")
                print("  - Different Bloomberg service")
                print("  - Special permissions")
                print("  - Alternative API approach")
            
            return results_file
            
        except Exception as e:
            print(f"ERROR: Failed to save membership results: {e}")
            return None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("SUCCESS: Bloomberg session disconnected")

def main():
    """Main execution function"""
    print("="*70)
    print("BLOOMBERG SPY MEMBERSHIP FIELDS TEST")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Testing ETF membership/constituent fields (replicating SPY EQUITY MEMB)...")
    
    tester = SPYMembershipTester()
    
    try:
        if not tester.connect():
            return False
        
        # Test membership fields
        results = tester.test_membership_fields()
        
        # Save results
        tester.save_membership_results(results)
        
        print(f"\nMembership field testing completed!")
        print("If successful fields found, we can use them to get SPY holdings.")
        return True
        
    except Exception as e:
        print(f"ERROR: Error in main execution: {e}")
        return False
    
    finally:
        tester.disconnect()

if __name__ == "__main__":
    main()