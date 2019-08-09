import xml.etree.cElementTree as ET
import urllib2
import psycopg2

#clear connection to postgres
conn = None

conn = psycopg2.connect(host="172.29.10.241", database="nzau_analytics", user="rtadmin", password="123")
print("Database Connected")
cur = conn.cursor()
rowcount = cur.rowcount



#Reference :
# https://docs.python.org/3.4/library/xml.etree.elementtree.html
# http://docs.python-guide.org/en/latest/scenarios/xml/
# https://anenadic.github.io/2014-11-10-manchester/novice/python/xml/xml.html

#http://stackabuse.com/reading-and-writing-xml-files-in-python/ ( good one )

#http://www.bom.gov.au/catalogue/data-feeds.shtml#xml   --Observations - State/Territory summaries

#tree = ET.ElementTree(file=urllib2.urlopen('ftp://ftp.bom.gov.au/anon/gen/fwo/IDQ60920.xml'))

#Northern  territory
tree = ET.ElementTree(file=urllib2.urlopen('ftp://ftp.bom.gov.au/anon/gen/fwo/IDD60920.xml'))

root = tree.getroot()

# for child in root:
#     print(child.tag, child.attrib)
#
# station = []
# for item in root.findall()


# root[0] =
# root[1] =observations
# root[1][1] = station

#station 1 position = root [1][0]
#station 2 position = root [1][1]
#station 3 position = root [1][2]
#print(root[1][1].attrib)

#loop through all station in observation dict
for station in root[1]:
    list = station.attrib
     #print(list)




#  loop through the period dict in each station

for station in root[1]:
    wmoid = station.attrib['wmo-id']
    bomid = station.attrib['bom-id']
    tz = station.attrib['tz']
    stnname = station.attrib['stn-name']
    stnheight = station.attrib['stn-height']
    stationtype = station.attrib['type']
    lat = station.attrib['lat']
    lon = station.attrib['lon']
    forecastdistrictid = station.attrib['forecast-district-id']
    description = station.attrib['description']

    windsrc = None
    for perioddata in station:
        periodindex = perioddata.attrib['index']
        timeutc = perioddata.attrib['time-utc']
        newtimeutc = timeutc.replace('T',' ').replace('+', ' +')
        timelocal = perioddata.attrib['time-local']
        newtimelocal = timelocal.replace('T',' ').replace('+', ' +')
        if 'wind-src' in perioddata.attrib:
            windsrc = perioddata.attrib['wind-src']


        for leveldata in perioddata:
            levelindex = leveldata.attrib['index']
            leveltype = leveldata.attrib['type']

            apparenttemp = "0"
            cloud = None
            cloud_base_m = "0"
            cloud_oktas = "0"
            cloud_type_id = "0"
            cloud_type = None
            delta_t = "0"
            gust_kmh = "0"
            wind_gust_spd = "0"
            air_temperature = "0"
            dew_point = "0"
            pres = "0"
            msl_pres = "0"
            qnh_pres = "0"
            rain_hour = "0"
            swell_dir = None
            swell_height = "0"
            swell_period = "0"
            rain_ten = "0"
            rel_humidity = "0"
            sea_height = "0"
            vis_km = "0"
            weather = None
            wind_dir = None
            wind_dir_deg = "0"
            wind_spd_kmh = "0"
            wind_spd = "0"
            rainfall = "0"
            rainfall_24hr = "0"
            maximum_air_temperature = "0"
            minimum_air_temperature = "0"
            maximum_gust_spd = "0"
            maximum_gust_kmh = "0"
            maximum_gust_dir = None

            for elementdata in leveldata:
                if 'apparent_temp' == elementdata.attrib['type']:
                    apparenttemp = elementdata.text
                if 'cloud' == elementdata.attrib['type']:
                    cloud = elementdata.text
                if 'cloud_base_m' == elementdata.attrib['type']:
                    cloud_base_m = elementdata.text
                if 'cloud_oktas' == elementdata.attrib['type']:
                    cloud_oktas = elementdata.text
                if 'cloud_type_id' == elementdata.attrib['type']:
                    cloud_type_id = elementdata.text
                if 'cloud_type' == elementdata.attrib['type']:
                    cloud_type = elementdata.text
                if 'delta_t' == elementdata.attrib['type']:
                    delta_t = elementdata.text
                if 'gust_kmh' == elementdata.attrib['type']:
                    gust_kmh = elementdata.text
                if 'wind_gust_spd' == elementdata.attrib['type']:
                    wind_gust_spd = elementdata.text
                if 'air_temperature' == elementdata.attrib['type']:
                    air_temperature = elementdata.text
                if 'dew_point' == elementdata.attrib['type']:
                    dew_point = elementdata.text
                if 'pres' == elementdata.attrib['type']:
                    pres = elementdata.text
                if 'msl_pres' == elementdata.attrib['type']:
                    msl_pres = elementdata.text
                if 'qnh_pres' == elementdata.attrib['type']:
                    qnh_pres = elementdata.text
                if 'rain_hour' == elementdata.attrib['type']:
                    rain_hour = elementdata.text
                if 'swell_dir' == elementdata.attrib['type']:
                    swell_dir = elementdata.text
                if 'swell_height' == elementdata.attrib['type']:
                    swell_height = elementdata.text
                if 'swell_period' == elementdata.attrib['type']:
                    swell_period = elementdata.text
                if 'rain_ten' == elementdata.attrib['type']:
                    rain_ten = elementdata.text
                if 'rel-humidity' == elementdata.attrib['type']:
                    rel_humidity = elementdata.text
                if 'sea_height' == elementdata.attrib['type']:
                    sea_height = elementdata.text
                if 'vis_km' == elementdata.attrib['type']:
                    vis_km = elementdata.text
                if 'weather' == elementdata.attrib['type']:
                    weather = elementdata.text
                if 'wind_dir' == elementdata.attrib['type']:
                    wind_dir = elementdata.text
                if 'wind_dir_deg' == elementdata.attrib['type']:
                    wind_dir_deg = elementdata.text
                if 'wind_spd_kmh' == elementdata.attrib['type']:
                    wind_spd_kmh = elementdata.text
                if 'wind_spd' == elementdata.attrib['type']:
                    wind_spd = elementdata.text
                if 'rainfall' == elementdata.attrib['type']:
                    rainfall = elementdata.text
                if 'rainfall_24hr' == elementdata.attrib['type']:
                    rainfall_24hr = elementdata.text
                if 'maximum_air_temperature' == elementdata.attrib['type']:
                    maximum_air_temperature = elementdata.text
                if 'minimum_air_temperature' == elementdata.attrib['type']:
                    minimum_air_temperature = elementdata.text
                if 'maximum_gust_spd' == elementdata.attrib['type']:
                    maximum_gust_spd = elementdata.text
                if 'maximum_gust_kmh' == elementdata.attrib['type']:
                    maximum_gust_kmh = elementdata.text
                if 'maximum_gust_dir' == elementdata.attrib['type']:
                    maximum_gust_dir = elementdata.text

            state  =  "Northern Territory"
            stndata = [ wmoid,
                        bomid,
                        tz,
                        stnname,
                        stnheight,
                        stationtype,
                        lat,
                        lon,
                        state,
                        forecastdistrictid,
                        description,
                        periodindex,
                        timeutc,
                        timelocal,
                        windsrc,
                        levelindex,
                        leveltype,
                        apparenttemp,
                        cloud,
                        cloud_base_m,
                        cloud_oktas,
                        cloud_type_id,
                        cloud_type,
                        delta_t,
                        gust_kmh,
                        wind_gust_spd,
                        air_temperature,
                        dew_point,
                        pres,
                        msl_pres,
                        qnh_pres,
                        rain_hour,
                        swell_dir,
                        swell_height,
                        swell_period,
                        rain_ten,
                        rel_humidity,
                        sea_height,
                        vis_km,
                        weather,
                        wind_dir,
                        wind_dir_deg,
                        wind_spd_kmh,
                        wind_spd,
                        rainfall,
                        rainfall_24hr,
                        maximum_air_temperature,
                        minimum_air_temperature,
                        maximum_gust_spd,
                        maximum_gust_kmh,
                        maximum_gust_dir]

            cur.execute(""" Insert into weather_aus 
                          (
                            wmo_id,
                            bom_id,
                            tz,
                            station_name,
                            station_height,
                            station_type,
                            latitude,
                            longitude,
                            state,
                            forecast_district_id,
                            description,
                            period_index,
                            time_utc,
                            time_local,
                            wind_src,
                            level_index,
                            level_type,
                            apparent_temp,
                            cloud,
                            cloud_base_m,
                            cloud_oktas,
                            cloud_type_id,
                            cloud_type,
                            delta_t,
                            gust_kmh,
                            wind_gust_spd,
                            air_temperature,
                            dew_point,
                            pres,
                            msl_pres,
                            qnh_pres,
                            rain_hour,
                            swell_dir,
                            swell_height,
                            swell_period,
                            rain_ten,
                            rel_humidity,
                            sea_height,
                            vis_km,
                            weather,
                            wind_dir,
                            wind_dir_deg,
                            wind_spd_kmh,
                            wind_spd,
                            rainfall,
                            rainfall_24hr,
                            maximum_air_temperature,
                            minimum_air_temperature,
                            maximum_gust_spd,
                            maximum_gust_kmh,
                            maximum_gust_dir
                          ) 
                          VALUES (
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s
                                  )
                                  """,(stndata))
            print("completed")
            conn.commit()
            stndata = []


conn.commit()
cur.close()
