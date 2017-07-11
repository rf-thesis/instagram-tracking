import arrow
import requests
from models import *
import config

hashtags = ["rf17", "rf2017", "roskilde", "orangefeeling", "rfrising"]

time = arrow.utcnow().replace(minutes=-90)
time = time.to('Europe/Copenhagen').format('YYYY-MM-DD HH:mm:ss')
print("Loading from %s\n" % time)
time = "2017-06-01 23:04:45"

url = "http://graflr.co/api/export-hashtags/"
token = config.api_token
db_eng = db_connect()
db_session = create_db_session(db_eng)
# check_index()

for h in hashtags:
    print("\n\nGetting hashtag: %s\n" % h)
    req = url + h + "?access_token=" + token + "&from=" + time
    data = requests.get(req).json()
    for d in data:
        print("Loading %s" % d.get("media_id"))
        try:
            d["hashtags"] = [i for i in d.get("media_text").split() if i.startswith("#")]
        except:
            d["hashtags"] = []
        p = arrow.get(d.get("created_time")).format("YYYY-MM-DDTHH:mm:ss+02:00")
        insta = Insta(media_id=d.get("media_id"),
                    user_id=d.get("user_id"),
                    username=d.get("username"),
                    fullname=d.get("full_name"),
                    predicted_gender=d.get("predicted_gender"),
                    created_time=d.get("created_time"),
                    like_count=d.get("like_count"),
                    comment_count=d.get("comment_count"),
                    media_text=d.get("media_text"),
                    hashtags=d.get("hashtags"),
                    photo_url=d.get("photo_url"),
                    location_name=d.get("location_name"),
                    longitude=d.get("longitude"),
                    latitude=d.get("latitude"),
                    location_id=d.get("location_id"),
                    country=d.get("country"))
        d["created_time"] = p
        if d.get("latitude"):
            loc = [float(d.get("longitude")), float(d.get("latitude"))]
            d["coordinates"] = {"text": d.get("location_name"), "location": loc}
            d["pin"] = loc
        db_session.merge(insta)
        load_es(d)
        r = requests.get(d.get("photo_url"), stream=True)
        if r.status_code == 200:
            with open(config.imgpath + d.get("media_id") + ".png", 'wb') as f:
            # with open("pics/%s.png" % d.get("media_id"), 'wb') as f:
                for chunk in r:
                    f.write(chunk)

    
    if data:
        d = data[-1]
        r = requests.get(d.get("photo_url"), stream=True)
        if r.status_code == 200:
          with open(config.imgpath + d.get("media_id") + ".png", 'wb') as f:
          # with open("pics/latest.png", 'wb') as f:
              for chunk in r:
                  f.write(chunk)
    db_session.commit()
