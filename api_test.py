from soda import SODA, GeoCode

addy = GeoCode(address="1229 W. Jarvis Chicago, IL 60626")

s = SODA()

print s.rodent_baiting(lat=addy.latitude, lng=addy.longitude, rad=200).rows
print s.police_stations(lat=addy.latitude, lng=addy.longitude, rad=2000).rows
print s.fire_stations(lat=addy.latitude, lng=addy.longitude, rad=2000).rows
print s.cta_stops(lat=addy.latitude, lng=addy.longitude, rad=2000).rows
print s.crimes(lat=addy.latitude, lng=addy.longitude, rad=60).rows
print s.libraries(lat=addy.latitude, lng=addy.longitude, rad=2000).rows
print s.schools(lat=addy.latitude, lng=addy.longitude, rad=2000).rows
print s.sex_offenders(lat=addy.latitude, lng=addy.longitude, rad=1000).rows
print s.parks(lat=addy.latitude, lng=addy.longitude, rad=300).rows
print s.farmers_market(lat=addy.latitude, lng=addy.longitude, rad=2000).rows
print s.graffiti_removal(lat=addy.latitude, lng=addy.longitude, rad=100).rows
print s.sanitation_complaints(lat=addy.latitude, lng=addy.longitude, rad=200).rows