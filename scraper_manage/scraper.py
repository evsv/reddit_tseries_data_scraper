import praw

def init_scraper(client_id, client_secret, user_agent):

    scraper_obj = praw.Reddit(client_id="8-Tp6GopEYaR6bWw162gQA",
                              client_secret="TjvFsMIxw484KaHSa2LE-IaITjG-Xg",
                              user_agent="temp")
    
    return scraper_obj