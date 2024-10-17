import io
import os
import threading
import requests
import polars as pl
from dotenv import load_dotenv
from datetime import datetime, timedelta
from queue import Queue
from ratelimit import limits, sleep_and_retry

load_dotenv()
CALLS = 50
RATE_LIMIT = 60

class SmartLead:
    def __init__(self):
        self.API_KEY = os.getenv("API_KEY")
        self.campaignQueue = Queue()
        self.leadQueue = Queue()
        self.stop_event = threading.Event()
        self.campaign_thread = threading.Thread(target=self.addToLeadQueue)
        self.lead_thread = threading.Thread(target=self.processLeadQueue)

    @sleep_and_retry
    @limits(calls=CALLS, period=RATE_LIMIT)
    def request(self, method, url):
        try:
            if method == "GET":
                response = requests.get(url)
            else:
                response = requests.delete(url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Error making API GET request to {url}: {e}")
            return None

    def get_message_history(self, campaign_id, lead_id):
        url = (
            f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/leads/"
            f"{lead_id}/message-history?api_key={self.API_KEY}"
        )
        response = self.request("GET", url)
        if response:
            return response.json()
        else:
            return {}

    def processLeadQueue(self):
        while not self.stop_event.is_set():
            try:
                message = self.leadQueue.get(timeout=1)
                if message is None:
                    self.leadQueue.task_done()
                emails = self.get_message_history(
                    message["campaign_id"], message["lead_id"]
                ).get("history", [])
                reply = any(email["type"] == "REPLY" for email in emails)
                if not reply and datetime.now() - datetime.strptime(
                    emails[-1]["time"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ) > timedelta(days=7):
                    print(
                        f"Lead {message['lead_id']} has not replied and the last message was over 7 days ago."
                    )
                    self._delete_leads(message["campaign_id"], message["lead_id"])
                self.leadQueue.task_done()
            except Exception:
                continue  # Handle queue timeout or other exceptions

    def _get_lead_ids(self, campaign_id):
        url = (
            f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/leads-export"
            f"?api_key={self.API_KEY}"
        )
        response = self.request("GET", url)
        if response:
            try:
                data = pl.read_csv(io.StringIO(response.text))
                filtered_data = data.filter(pl.col("status") == "COMPLETED").select(
                    "id"
                )
                print(f"Found {len(filtered_data)} leads for campaign {campaign_id}")
                return filtered_data["id"].to_list()
            except Exception as e:
                print(f"Error processing leads data for campaign {campaign_id}: {e}")
                return []
        else:
            return []

    def addToLeadQueue(self):
        while not self.stop_event.is_set():
            try:
                campaign = self.campaignQueue.get(timeout=1)
                if campaign is None:
                    self.campaignQueue.task_done()
                    break  # Exit loop when sentinel is received
                # print(f"Fetching leads for {campaign['id']} - {campaign['name']} - {campaign['status']}")
                leads = self._get_lead_ids(campaign["id"])
                for lead_id in leads:
                    self.leadQueue.put(
                        {"campaign_id": campaign["id"], "lead_id": lead_id}
                    )
                self.campaignQueue.task_done()
            except Exception:
                continue  # Handle queue timeout or other exceptions
        # Signal that no more leads will be added
        self.leadQueue.put(None)

    def _get_campaigns(self):
        print("Fetching completed campaigns")
        url = f"https://server.smartlead.ai/api/v1/campaigns?api_key={self.API_KEY}"
        response = self.request("GET", url)
        if response:
            try:
                campaigns = [x for x in response.json() if x["status"] == "COMPLETED"]
                print(f"Found {len(campaigns)} completed campaigns.")
                return campaigns
            except Exception as e:
                print(f"Error processing campaigns data: {e}")
                return []
        else:
            return []

    def _delete_leads(self, campaign_id, lead_id):
        self.request(
            "DELETE",
            f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/leads/{lead_id}?api_key={self.API_KEY}",
        )

    def execute(self):
        try:
            campaigns = self._get_campaigns()
            if not campaigns:
                print("No campaigns found")
                return

            self.lead_thread.start()  # Start lead processing thread first
            self.campaign_thread.start()  # Start campaign processing thread
            count = 0
            for campaign in campaigns:
                self.campaignQueue.put(campaign)
                if(count == 2):
                    break
                count += 1

            self.campaignQueue.put(None)  # Signal completion to campaign thread
            self.campaignQueue.join()  # Wait for all campaigns to be processed

            self.leadQueue.join()  # Wait for all leads to be processed

        except KeyboardInterrupt:
            print("KeyboardInterrupt received, stopping...")
            self.stop_event.set()  # Signal threads to stop
        finally:
            self.stop_event.set()  # Ensure threads are signaled to stop
            self.campaignQueue.put(None)  # Ensure campaign thread can exit
            self.leadQueue.put(None)  # Ensure lead thread can exit
            self.campaign_thread.join()  # Ensure campaign thread has stopped
            self.lead_thread.join()  # Ensure lead thread has stopped
            print("All tasks completed")


if __name__ == "__main__":
    try:
        smart_lead = SmartLead()
        smart_lead.execute()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Cleaning up resources...")
