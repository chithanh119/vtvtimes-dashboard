import os
import requests
from datetime import datetime
from dotenv import load_dotenv, set_key

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(SCRIPT_DIR, ".env")

# Load environment variables from script directory
load_dotenv(ENV_FILE)

class FacebookTokenManager:
    def __init__(self, env_file=None):
        self.app_id = os.getenv("FACEBOOK_APP_ID")
        self.app_secret = os.getenv("FACEBOOK_APP_SECRET")
        self.user_token = os.getenv("FACEBOOK_USER_ACCESS_TOKEN")
        self.page_id = os.getenv("FACEBOOK_PAGE_ID")
        self.api_version = os.getenv("FACEBOOK_API_VERSION")
        
        # Use the .env file in the script directory
        self.env_file = env_file if env_file else ENV_FILE
        
        print(f"Using .env file: {os.path.abspath(self.env_file)}")
    
    def exchange_for_long_lived_token(self, short_lived_token):
        """
        Exchange a short-lived token (60 days) for a long-lived token (60 days)
        """
        url = f"https://graph.facebook.com/{self.api_version}/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": self.app_id,
            "client_secret": self.app_secret,
            "fb_exchange_token": short_lived_token
        }
        
        print("Exchanging for long-lived user token...")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            long_lived_token = data.get("access_token")
            expires_in = data.get("expires_in", 0)
            
            print(f"✓ Long-lived token obtained (expires in {expires_in} seconds / ~{expires_in//86400} days)")
            return long_lived_token
        else:
            print(f"✗ Error: {response.text}")
            return None
    
    def get_page_access_token(self, user_token):
        """
        Get Page Access Token from User Access Token
        Page tokens don't expire as long as the user token is valid
        """
        url = f"https://graph.facebook.com/{self.api_version}/me/accounts"
        params = {"access_token": user_token}
        
        print("\nGetting Page Access Token...")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            pages = data.get("data", [])
            
            for page in pages:
                print(f"  Found page: {page['name']} (ID: {page['id']})")
                if page['id'] == self.page_id:
                    page_token = page['access_token']
                    print(f"  ✓ Page Access Token obtained for {page['name']}")
                    return page_token
            
            print(f"✗ Page ID {self.page_id} not found")
            return None
        else:
            print(f"✗ Error: {response.text}")
            return None
    
    def verify_token(self, token):
        """
        Verify token and get its details
        """
        url = f"https://graph.facebook.com/{self.api_version}/debug_token"
        params = {
            "input_token": token,
            "access_token": f"{self.app_id}|{self.app_secret}"
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json().get("data", {})
            return data
        return None
    
    def update_env_file(self, key, value):
        """
        Update .env file with new token
        """
        try:
            set_key(self.env_file, key, value)
            print(f"✓ Updated {key} in {self.env_file}")
            
            # Verify it was written
            load_dotenv(self.env_file, override=True)
            saved_value = os.getenv(key)
            if saved_value == value:
                print(f"  ✓ Verified: {key} saved correctly")
            else:
                print(f"  ⚠ Warning: {key} may not have saved correctly")
        except Exception as e:
            print(f"✗ Error updating {key}: {e}")
    
    def refresh_tokens(self):
        """
        Main method to refresh all tokens
        """
        print("="*60)
        print("FACEBOOK TOKEN REFRESH")
        print("="*60)
        
        # Step 1: Exchange short-lived for long-lived user token
        if self.user_token:
            token_info = self.verify_token(self.user_token)
            
            if token_info and token_info.get("is_valid"):
                print(f"\nCurrent user token is valid")
                print(f"  Type: {token_info.get('type')}")
                print(f"  Expires: {datetime.fromtimestamp(token_info.get('expires_at', 0))}")
                
                # Check if token expires soon (less than 7 days)
                expires_at = token_info.get("expires_at", 0)
                days_until_expiry = (expires_at - datetime.now().timestamp()) / 86400
                
                if days_until_expiry < 7:
                    print(f"  ⚠ Token expires in {days_until_expiry:.1f} days - refreshing...")
                    long_lived_token = self.exchange_for_long_lived_token(self.user_token)
                    if long_lived_token:
                        self.user_token = long_lived_token
                        self.update_env_file("FACEBOOK_USER_ACCESS_TOKEN", long_lived_token)
                    else:
                        print(f"  ⚠ Token exchange failed, but continuing with current token...")
                else:
                    print(f"  ✓ Token is still valid for {days_until_expiry:.1f} days")
            else:
                print("\n✗ User token is invalid - please generate a new one from Graph API Explorer")
                return False
        
        # Step 2: Get Page Access Token
        page_token = self.get_page_access_token(self.user_token)
        
        if page_token:
            self.update_env_file("FACEBOOK_PAGE_ACCESS_TOKEN", page_token)
            
            # Verify the page token
            page_token_info = self.verify_token(page_token)
            if page_token_info:
                print(f"\nPage token details:")
                print(f"  Type: {page_token_info.get('type')}")
                print(f"  Valid: {page_token_info.get('is_valid')}")
                
                expires_at = page_token_info.get("expires_at", 0)
                if expires_at == 0:
                    print(f"  Expires: Never (as long as user token is valid)")
                else:
                    print(f"  Expires: {datetime.fromtimestamp(expires_at)}")
            
            print("\n" + "="*60)
            print("✓ TOKEN REFRESH COMPLETE")
            print("="*60)
            return True
        else:
            print("\n✗ Failed to get Page Access Token")
            return False

if __name__ == "__main__":
    manager = FacebookTokenManager()
    success = manager.refresh_tokens()
    
    if not success:
        print("\n" + "="*60)
        print("MANUAL STEPS REQUIRED:")
        print("="*60)
        print("1. Go to: https://developers.facebook.com/tools/explorer/")
        print("2. Select your app")
        print("3. Click 'Get Token' → 'Get User Access Token'")
        print("4. Select permissions: pages_read_engagement, pages_show_list, read_insights")
        print("5. Copy the token and add to .env as FACEBOOK_USER_ACCESS_TOKEN")
        print("6. Run this script again")
        print("="*60)