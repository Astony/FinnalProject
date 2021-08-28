from project.weather_script import forecast_weather, get_urls, prev_weather,filter_df_from_invalid_rows,geocoder_setup,define_address
from project import get_central_coord
from unittest.mock import patch
import pandas as pd
cities = ["FakeCity"]
coordinates = [[666,777]]

def mock_get_urls():
    return ["https://fakeURL.com/"]

get_urls = mock_get_urls

def test_weather_for_current_day_and_next_7_days():
    with patch("requests.get") as mock_request:
        mock_request.return_value.text = """{"lat":52.363,"lon":4.8875,"timezone":"Europe/Amsterdam","timezone_offset":7200,"current":{"dt":1630149358,"sunrise":1630125917,"sunset":1630175909,"temp":292.3,"feels_like":292.18,"pressure":1020,"humidity":73,"dew_point":287.35,"uvi":3.91,"clouds":20,"visibility":10000,"wind_speed":2.68,"wind_deg":23,"wind_gust":7.15,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02d"}]},"minutely":[{"dt":1630149360,"precipitation":0},{"dt":1630149420,"precipitation":0},{"dt":1630149480,"precipitation":0},{"dt":1630149540,"precipitation":0},{"dt":1630149600,"precipitation":0},{"dt":1630149660,"precipitation":0},{"dt":1630149720,"precipitation":0},{"dt":1630149780,"precipitation":0},{"dt":1630149840,"precipitation":0},{"dt":1630149900,"precipitation":0},{"dt":1630149960,"precipitation":0},{"dt":1630150020,"precipitation":0},{"dt":1630150080,"precipitation":0},{"dt":1630150140,"precipitation":0},{"dt":1630150200,"precipitation":0},{"dt":1630150260,"precipitation":0},{"dt":1630150320,"precipitation":0},{"dt":1630150380,"precipitation":0},{"dt":1630150440,"precipitation":0},{"dt":1630150500,"precipitation":0},{"dt":1630150560,"precipitation":0},{"dt":1630150620,"precipitation":0},{"dt":1630150680,"precipitation":0},{"dt":1630150740,"precipitation":0},{"dt":1630150800,"precipitation":0},{"dt":1630150860,"precipitation":0},{"dt":1630150920,"precipitation":0},{"dt":1630150980,"precipitation":0},{"dt":1630151040,"precipitation":0},{"dt":1630151100,"precipitation":0},{"dt":1630151160,"precipitation":0},{"dt":1630151220,"precipitation":0},{"dt":1630151280,"precipitation":0},{"dt":1630151340,"precipitation":0},{"dt":1630151400,"precipitation":0},{"dt":1630151460,"precipitation":0},{"dt":1630151520,"precipitation":0},{"dt":1630151580,"precipitation":0},{"dt":1630151640,"precipitation":0},{"dt":1630151700,"precipitation":0},{"dt":1630151760,"precipitation":0},{"dt":1630151820,"precipitation":0},{"dt":1630151880,"precipitation":0},{"dt":1630151940,"precipitation":0},{"dt":1630152000,"precipitation":0},{"dt":1630152060,"precipitation":0},{"dt":1630152120,"precipitation":0},{"dt":1630152180,"precipitation":0},{"dt":1630152240,"precipitation":0},{"dt":1630152300,"precipitation":0},{"dt":1630152360,"precipitation":0},{"dt":1630152420,"precipitation":0},{"dt":1630152480,"precipitation":0},{"dt":1630152540,"precipitation":0},{"dt":1630152600,"precipitation":0},{"dt":1630152660,"precipitation":0},{"dt":1630152720,"precipitation":0},{"dt":1630152780,"precipitation":0},{"dt":1630152840,"precipitation":0},{"dt":1630152900,"precipitation":0},{"dt":1630152960,"precipitation":0}],"daily":[{"dt":1630148400,"sunrise":1630125917,"sunset":1630175909,"moonrise":1630184100,"moonset":1630150020,"moon_phase":0.69,"temp":{"day":292.3,"min":287.03,"max":292.3,"night":287.44,"eve":289.27,"morn":287.63},"feels_like":{"day":292.18,"night":287.17,"eve":288.92,"morn":287.51},"pressure":1020,"humidity":73,"dew_point":287.35,"wind_speed":6.66,"wind_deg":17,"wind_gust":11.25,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02d"}],"clouds":20,"pop":0.03,"uvi":4.02},{"dt":1630234800,"sunrise":1630212417,"sunset":1630262174,"moonrise":1630271700,"moonset":1630240620,"moon_phase":0.72,"temp":{"day":288.6,"min":285.6,"max":290.74,"night":287.43,"eve":289.76,"morn":285.87},"feels_like":{"day":288.37,"night":287.39,"eve":289.7,"morn":285.52},"pressure":1020,"humidity":83,"dew_point":285.65,"wind_speed":5.71,"wind_deg":354,"wind_gust":11.44,"weather":[{"id":804,"main":"Clouds","description":"overcast clouds","icon":"04d"}],"clouds":99,"pop":0.25,"uvi":3.17},{"dt":1630321200,"sunrise":1630298916,"sunset":1630348438,"moonrise":1630359660,"moonset":1630331220,"moon_phase":0.75,"temp":{"day":293.46,"min":286.84,"max":293.9,"night":286.84,"eve":290.85,"morn":287.62},"feels_like":{"day":293.24,"night":286.69,"eve":290.63,"morn":287.6},"pressure":1020,"humidity":65,"dew_point":286.56,"wind_speed":6.28,"wind_deg":7,"wind_gust":11.83,"weather":[{"id":804,"main":"Clouds","description":"overcast clouds","icon":"04d"}],"clouds":95,"pop":0.18,"uvi":4.39},{"dt":1630407600,"sunrise":1630385415,"sunset":1630434702,"moonrise":0,"moonset":1630421640,"moon_phase":0.78,"temp":{"day":293.14,"min":284.71,"max":293.14,"night":284.71,"eve":287.09,"morn":286.52},"feels_like":{"day":292.71,"night":284.25,"eve":286.65,"morn":286.24},"pressure":1024,"humidity":58,"dew_point":284.56,"wind_speed":5.49,"wind_deg":346,"wind_gust":8.65,"weather":[{"id":800,"main":"Clear","description":"clear sky","icon":"01d"}],"clouds":2,"pop":0,"uvi":4.4},{"dt":1630494000,"sunrise":1630471915,"sunset":1630520965,"moonrise":1630448040,"moonset":1630511640,"moon_phase":0.81,"temp":{"day":292.11,"min":285.6,"max":292.62,"night":285.6,"eve":288.39,"morn":287.55},"feels_like":{"day":291.58,"night":285.28,"eve":288.01,"morn":286.93},"pressure":1026,"humidity":58,"dew_point":283.72,"wind_speed":3.26,"wind_deg":349,"wind_gust":5.67,"weather":[{"id":804,"main":"Clouds","description":"overcast clouds","icon":"04d"}],"clouds":88,"pop":0,"uvi":2.91},{"dt":1630580400,"sunrise":1630558414,"sunset":1630607227,"moonrise":1630537200,"moonset":1630601040,"moon_phase":0.85,"temp":{"day":293.26,"min":284.78,"max":293.26,"night":285.43,"eve":288.11,"morn":285.3},"feels_like":{"day":292.74,"night":285.14,"eve":287.72,"morn":285.03},"pressure":1026,"humidity":54,"dew_point":283.68,"wind_speed":3.54,"wind_deg":347,"wind_gust":4.29,"weather":[{"id":800,"main":"Clear","description":"clear sky","icon":"01d"}],"clouds":2,"pop":0,"uvi":3},{"dt":1630666800,"sunrise":1630644913,"sunset":1630693489,"moonrise":1630627020,"moonset":1630689840,"moon_phase":0.88,"temp":{"day":293.84,"min":284.43,"max":294.24,"night":286.61,"eve":289.51,"morn":285.05},"feels_like":{"day":293.37,"night":286.34,"eve":289.24,"morn":284.78},"pressure":1021,"humidity":54,"dew_point":284.13,"wind_speed":4.02,"wind_deg":14,"wind_gust":8.81,"weather":[{"id":800,"main":"Clear","description":"clear sky","icon":"01d"}],"clouds":6,"pop":0,"uvi":3},{"dt":1630753200,"sunrise":1630731412,"sunset":1630779750,"moonrise":1630717560,"moonset":1630778100,"moon_phase":0.91,"temp":{"day":292.58,"min":284.56,"max":292.58,"night":286.9,"eve":288.77,"morn":285.03},"feels_like":{"day":292.07,"night":286.76,"eve":288.58,"morn":284.83},"pressure":1020,"humidity":57,"dew_point":283.7,"wind_speed":4.13,"wind_deg":14,"wind_gust":7.75,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02d"}],"clouds":16,"pop":0,"uvi":3}]}"""
        df = forecast_weather(cities, coordinates)
        assert len(df) == 8

def test_weather_for_previous_5_days():
    with patch("requests.get") as mock_request:
        mock_request.return_value.text = """{"lat":52.363,"lon":4.8875,"timezone":"Europe/Amsterdam","timezone_offset":7200,"current":{"dt":1629967939,"sunrise":1629952918,"sunset":1630003377,"temp":289.77,"feels_like":289.63,"pressure":1015,"humidity":82,"dew_point":286.69,"uvi":1.88,"clouds":75,"visibility":10000,"wind_speed":0.89,"wind_deg":283,"wind_gust":2.68,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}]},"hourly":[{"dt":1629936000,"temp":288.79,"feels_like":288.71,"pressure":1016,"humidity":88,"dew_point":286.81,"uvi":0,"clouds":75,"visibility":10000,"wind_speed":0.45,"wind_deg":321,"wind_gust":2.68,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04n"}]},{"dt":1629939600,"temp":288.66,"feels_like":288.46,"pressure":1016,"humidity":84,"dew_point":285.97,"uvi":0,"clouds":75,"visibility":10000,"wind_speed":0.45,"wind_deg":39,"wind_gust":2.68,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04n"}]},{"dt":1629943200,"temp":288.41,"feels_like":288.13,"pressure":1016,"humidity":82,"dew_point":285.36,"uvi":0,"clouds":75,"visibility":10000,"wind_speed":1.79,"wind_deg":356,"wind_gust":3.13,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04n"}]},{"dt":1629946800,"temp":288.3,"feels_like":287.99,"pressure":1015,"humidity":81,"dew_point":285.07,"uvi":0,"clouds":75,"visibility":10000,"wind_speed":2.24,"wind_deg":341,"wind_gust":2.68,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04n"}]},{"dt":1629950400,"temp":288.25,"feels_like":287.9,"pressure":1015,"humidity":80,"dew_point":284.83,"uvi":0,"clouds":75,"visibility":10000,"wind_speed":1.79,"wind_deg":350,"wind_gust":3.58,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04n"}]},{"dt":1629954000,"temp":288.29,"feels_like":288,"pressure":1015,"humidity":82,"dew_point":285.24,"uvi":0,"clouds":75,"visibility":10000,"wind_speed":1.79,"wind_deg":326,"wind_gust":2.24,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}]},{"dt":1629957600,"temp":288.48,"feels_like":288.18,"pressure":1015,"humidity":81,"dew_point":285.24,"uvi":0.11,"clouds":75,"visibility":10000,"wind_speed":2.24,"wind_deg":321,"wind_gust":4.02,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}]},{"dt":1629961200,"temp":288.81,"feels_like":288.55,"pressure":1015,"humidity":81,"dew_point":285.56,"uvi":0.51,"clouds":75,"visibility":10000,"wind_speed":0.45,"wind_deg":7,"wind_gust":2.24,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}]},{"dt":1629964800,"temp":289.3,"feels_like":289.19,"pressure":1015,"humidity":85,"dew_point":286.78,"uvi":1.11,"clouds":75,"visibility":10000,"wind_speed":0.45,"wind_deg":314,"wind_gust":2.68,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}]},{"dt":1629968400,"temp":290.2,"feels_like":290.05,"pressure":1015,"humidity":80,"dew_point":286.73,"uvi":1.88,"clouds":75,"visibility":10000,"wind_speed":0.89,"wind_deg":283,"wind_gust":2.68,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}]},{"dt":1629972000,"temp":290.4,"feels_like":290.19,"pressure":1015,"humidity":77,"dew_point":286.34,"uvi":3.15,"clouds":75,"visibility":10000,"wind_speed":0.89,"wind_deg":230,"wind_gust":3.58,"weather":[{"id":520,"main":"Rain","description":"light intensity shower rain","icon":"09d"}]},{"dt":1629975600,"temp":290.61,"feels_like":290.4,"pressure":1015,"humidity":76,"dew_point":286.34,"uvi":3.75,"clouds":75,"visibility":10000,"wind_speed":2.68,"wind_deg":344,"wind_gust":4.92,"weather":[{"id":520,"main":"Rain","description":"light intensity shower rain","icon":"09d"}]},{"dt":1629979200,"temp":290.89,"feels_like":290.76,"pressure":1016,"humidity":78,"dew_point":287.01,"uvi":3.85,"clouds":75,"visibility":10000,"wind_speed":0.89,"wind_deg":302,"wind_gust":4.02,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}]},{"dt":1629982800,"temp":290.93,"feels_like":290.85,"pressure":1015,"humidity":80,"dew_point":287.44,"uvi":3.2,"clouds":40,"visibility":10000,"wind_speed":0.89,"wind_deg":235,"wind_gust":3.58,"weather":[{"id":500,"main":"Rain","description":"light rain","icon":"10d"}],"rain":{"1h":0.38}},{"dt":1629986400,"temp":292.2,"feels_like":292.09,"pressure":1015,"humidity":74,"dew_point":287.46,"uvi":2.45,"clouds":75,"visibility":10000,"wind_speed":0.89,"wind_deg":308,"wind_gust":4.02,"weather":[{"id":803,"main":"Clouds","description":"broken clouds","icon":"04d"}]},{"dt":1629990000,"temp":292.39,"feels_like":292.28,"pressure":1015,"humidity":73,"dew_point":287.44,"uvi":1.58,"clouds":20,"visibility":10000,"wind_speed":1.34,"wind_deg":346,"wind_gust":3.13,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02d"}]},{"dt":1629993600,"temp":292.27,"feels_like":292.17,"pressure":1015,"humidity":74,"dew_point":287.53,"uvi":1.13,"clouds":20,"visibility":10000,"wind_speed":1.34,"wind_deg":6,"wind_gust":3.58,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02d"}]},{"dt":1629997200,"temp":291.5,"feels_like":291.22,"pressure":1015,"humidity":70,"dew_point":285.94,"uvi":0.43,"clouds":20,"visibility":10000,"wind_speed":2.24,"wind_deg":347,"wind_gust":6.71,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02d"}]},{"dt":1630000800,"temp":290.73,"feels_like":290.42,"pressure":1016,"humidity":72,"dew_point":285.63,"uvi":0,"clouds":20,"visibility":10000,"wind_speed":2.24,"wind_deg":9,"wind_gust":4.47,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02d"}]},{"dt":1630004400,"temp":289.2,"feels_like":288.82,"pressure":1017,"humidity":75,"dew_point":284.77,"uvi":0,"clouds":20,"visibility":10000,"wind_speed":0.45,"wind_deg":3,"wind_gust":4.02,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02n"}]},{"dt":1630008000,"temp":288.71,"feels_like":288.33,"pressure":1017,"humidity":77,"dew_point":284.7,"uvi":0,"clouds":20,"visibility":10000,"wind_speed":2.24,"wind_deg":0,"wind_gust":3.58,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02n"}]},{"dt":1630011600,"temp":287.93,"feels_like":287.53,"pressure":1018,"humidity":79,"dew_point":284.33,"uvi":0,"clouds":20,"visibility":10000,"wind_speed":0.89,"wind_deg":352,"wind_gust":1.79,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02n"}]},{"dt":1630015200,"temp":287.18,"feels_like":286.81,"pressure":1018,"humidity":83,"dew_point":284.34,"uvi":0,"clouds":15,"visibility":10000,"wind_speed":1.34,"wind_deg":349,"wind_gust":2.24,"weather":[{"id":801,"main":"Clouds","description":"few clouds","icon":"02n"}]},{"dt":1630018800,"temp":286.7,"feels_like":286.33,"pressure":1017,"humidity":85,"dew_point":284.23,"uvi":0,"clouds":0,"visibility":10000,"wind_speed":0.45,"wind_deg":355,"wind_gust":0.89,"weather":[{"id":800,"main":"Clear","description":"clear sky","icon":"01n"}]}]}"""
        df = prev_weather(cities, coordinates)
        assert len(df) == 5


def test_filter_func():
    testing_df = pd.DataFrame({"Name":["21345", "Spb", "America", "", "Sincity"],
                   "Longitude":[100, 100, 10000, 100,100],
                   "Latitude":[0, 0, 0, 100000, 100]})
    result_df = filter_df_from_invalid_rows(testing_df)
    assert len(result_df) == 1
    assert result_df.Name.item() == "Spb"

def mock_geocoder_setup():
    return ["Test address"]

def test_define_addresses_function():
    geocoder_setup = mock_geocoder_setup
    test_df = pd.DataFrame({"Latitude":[0], "Longitude":[0]})
    result_df = define_address(test_df)
    assert result_df.Address




