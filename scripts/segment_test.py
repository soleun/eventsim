import dateutil.parser

import analytics
analytics.write_key = 'h0OpjxFW8jutWndfFRC2X2LyBLz6ZSzz'
analytics.debug = True

log = [
    '2012-10-17T18:58:57.911Z 019mr8mf4r /purchased/tshirt'
]

for entry in log:
    (timestamp_str, user_id, url) = entry.split(' ')
    timestamp = dateutil.parser.parse(timestamp_str)  # datetime.datetime object has a timezone

    # have a timezone? check yo'self
    assert timestamp.tzinfo

    analytics.track(user_id, 'Bought a shirt', {
        'color': 'Blue',
        'revenue': 17.90
    }, timestamp=timestamp)

analytics.flush()