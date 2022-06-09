import json
import scraper_manage.scraper as scrm

if __name__ == "__main__":

    reddit_info_config_fpath = "scraper_manage/reddit_config.json"
    auth_config_fpath = "scraper_manage/auth.json"

    with open(reddit_info_config_fpath, "r") as f:
        reddit_info_config = json.load(f)
        
    with open(auth_config_fpath) as f:
        auth_config = json.load(f)

    rdt_scraper = scrm.init_scraper(client_id=auth_config["client_id"],
                                    client_secret=auth_config["client_secret"],
                                    user_agent=auth_config["user_agent"])

