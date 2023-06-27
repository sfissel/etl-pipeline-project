
# Data Science Systems: Creating a segment of an ETL pipeline that will ingest and process raw data
## - Implementing data science systems rooted in SQL and other data sources like CSVs, Open Data, and other relational data sources, as well as APIs and data transformation
## Stephanie Fissel, October 20, 2022

## Import Packages


```python
import json
import csv
import requests
import pandas as pd
import sqlite3
```

## Fetch remote data file by URL

### Import urllib library


```python
from urllib.request import urlopen
```

### Store URL in url as parameter for urlopen


```python
url = "https://holidays.abstractapi.com/v1/?api_key=###&country=US&year=2020"
```

### Store the response of URL


```python
response = urlopen(url)
```

### Store the JSON response from url in data


```python
data_json = json.loads(response.read())
```

## Convert from JSON to SQLite database (with reduced number of columns from source to destination)


```python
api_url = requests.get('https://holidays.abstractapi.com/v1/?api_key=574b7d06a79c4794a2e390123a19b857&country=US&year=2020')
data_json = api_url.json()

connection = sqlite3.connect('holiday.sqlite')
cursor = connection.cursor()
cursor.execute('Create Table if not exists holiday (name TEXT, date TEXT, type TEXT, country TEXT)')

columns = ['name','date','type', 'country']
for row in data_json:
    keys= tuple(row[c] for c in columns)
    cursor.execute('insert into holiday values(?,?,?,?)',keys)
    print(f'{row["name"]} data inserted Succefully')

connection.commit()
connection.close()
```

    New Year's Day data inserted Succefully
    World Braille Day data inserted Succefully
    Epiphany data inserted Succefully
    International Programmers' Day data inserted Succefully
    Orthodox Christmas Day data inserted Succefully
    Stephen Foster Memorial Day data inserted Succefully
    Orthodox New Year data inserted Succefully
    Lee-Jackson Day data inserted Succefully
    Confederate Heroes' Day data inserted Succefully
    World Religion Day data inserted Succefully
    Robert E. Lee's Birthday data inserted Succefully
    Martin Luther King Jr. Day data inserted Succefully
    Civil Rights Day data inserted Succefully
    Robert E. Lee's Birthday data inserted Succefully
    Civil Rights Day data inserted Succefully
    Robert E. Lee's Birthday data inserted Succefully
    Robert E. Lee's Birthday data inserted Succefully
    Idaho Human Rights Day data inserted Succefully
    Chinese New Year data inserted Succefully
    World Leprosy Day data inserted Succefully
    International Customs Day data inserted Succefully
    International Day of Commemoration in Memory of the Victims of the Holocaust data inserted Succefully
    Kansas Day data inserted Succefully
    National Freedom Day data inserted Succefully
    Groundhog Day data inserted Succefully
    Super Bowl data inserted Succefully
    World Wetlands Day data inserted Succefully
    Rosa Parks Day data inserted Succefully
    World Cancer Day data inserted Succefully
    Rosa Parks Day data inserted Succefully
    International Day of Zero Tolerance for Female Genital Mutilation data inserted Succefully
    National Wear Red Day data inserted Succefully
    Tu Bishvat/Tu B'Shevat data inserted Succefully
    World Day of the Sick data inserted Succefully
    International Day of Women and Girls in Science data inserted Succefully
    Lincoln's Birthday data inserted Succefully
    Lincoln's Birthday data inserted Succefully
    World Radio Day data inserted Succefully
    Statehood Day data inserted Succefully
    Valentine's Day data inserted Succefully
    Susan B. Anthony's Birthday data inserted Succefully
    Susan B. Anthony's Birthday data inserted Succefully
    Susan B. Anthony's Birthday data inserted Succefully
    Susan B. Anthony's Birthday data inserted Succefully
    Elizabeth Peratrovich Day data inserted Succefully
    Presidents' Day data inserted Succefully
    Daisy Gatson Bates Day data inserted Succefully
    World Day of Social Justice data inserted Succefully
    Maha Shivaratri data inserted Succefully
    International Mother Language Day data inserted Succefully
    Shrove Tuesday/Mardi Gras data inserted Succefully
    Shrove Tuesday/Mardi Gras data inserted Succefully
    Shrove Tuesday/Mardi Gras data inserted Succefully
    Shrove Tuesday/Mardi Gras data inserted Succefully
    Ash Wednesday data inserted Succefully
    Linus Pauling Day data inserted Succefully
    Leap Day data inserted Succefully
    St. David's Day data inserted Succefully
    Zero Discrimination Day data inserted Succefully
    Self-Injury Awareness Day data inserted Succefully
    Casimir Pulaski Day data inserted Succefully
    Read Across America Day data inserted Succefully
    Texas Independence Day data inserted Succefully
    World Wildlife Day data inserted Succefully
    Town Meeting Day data inserted Succefully
    Super Tuesday data inserted Succefully
    Employee Appreciation Day data inserted Succefully
    Daylight Saving Time starts data inserted Succefully
    International Women's Day data inserted Succefully
    Holi data inserted Succefully
    Purim data inserted Succefully
    World Kidney Day data inserted Succefully
    Friday the 13th data inserted Succefully
    Evacuation Day data inserted Succefully
    St. Patrick's Day data inserted Succefully
    March Equinox data inserted Succefully
    International Day of Happiness data inserted Succefully
    World Down Syndrome Day data inserted Succefully
    International Day of Forests data inserted Succefully
    International Day for the Elimination of Racial Discrimination data inserted Succefully
    World Poetry Day data inserted Succefully
    International Day of Nowruz data inserted Succefully
    Isra and Mi'raj data inserted Succefully
    World Water Day data inserted Succefully
    World Meteorological Day data inserted Succefully
    World Tuberculosis Day data inserted Succefully
    International Day for the Right to the Truth concerning Gross Human Rights Violations and for the Dignity of Victims data inserted Succefully
    International Day of Solidarity with Detained and Missing Staff Members data inserted Succefully
    Maryland Day data inserted Succefully
    International Day of Remembrance of Slavery Victims and the Transatlantic Slave Trade data inserted Succefully
    Prince Jonah Kuhio Kalanianaole Day data inserted Succefully
    Earth Hour data inserted Succefully
    National Vietnam War Veterans Day data inserted Succefully
    Seward's Day data inserted Succefully
    César Chávez Day data inserted Succefully
    César Chávez Day data inserted Succefully
    César Chávez Day data inserted Succefully
    César Chávez Day data inserted Succefully
    April Fool's Day data inserted Succefully
    Pascua Florida Day data inserted Succefully
    World Autism Awareness Day data inserted Succefully
    United Nations' Mine Awareness Day data inserted Succefully
    Palm Sunday data inserted Succefully
    National Tartan Day data inserted Succefully
    International Day of Sport for Development and Peace data inserted Succefully
    Day of Remembrance of the Victims of the Rwanda Genocide data inserted Succefully
    United Nations' World Health Day data inserted Succefully
    Passover (first day) data inserted Succefully
    Maundy Thursday data inserted Succefully
    Good Friday data inserted Succefully
    Holy Saturday data inserted Succefully
    Easter Sunday data inserted Succefully
    International Day of Human Space Flight data inserted Succefully
    Easter Monday data inserted Succefully
    Thomas Jefferson's Birthday data inserted Succefully
    Father Damien Day data inserted Succefully
    Tax Day data inserted Succefully
    Last Day of Passover data inserted Succefully
    Emancipation Day data inserted Succefully
    Orthodox Good Friday data inserted Succefully
    Orthodox Holy Saturday data inserted Succefully
    International Day for Monuments and Sites data inserted Succefully
    Orthodox Easter data inserted Succefully
    Boston Marathon data inserted Succefully
    Chinese Language Day data inserted Succefully
    Orthodox Easter Monday data inserted Succefully
    Patriot's Day data inserted Succefully
    Yom HaShoah data inserted Succefully
    National Library Workers' Day data inserted Succefully
    San Jacinto Day data inserted Succefully
    Oklahoma Day data inserted Succefully
    Earth Day data inserted Succefully
    Administrative Professionals Day data inserted Succefully
    Take our Daughters and Sons to Work Day data inserted Succefully
    World Book and Copyright Day data inserted Succefully
    English Language Day data inserted Succefully
    Ramadan Starts data inserted Succefully
    Arbor Day data inserted Succefully
    World Malaria Day data inserted Succefully
    World Intellectual Property Day data inserted Succefully
    International Chernobyl Disaster Remembrance Day data inserted Succefully
    Confederate Heroes' Day data inserted Succefully
    Confederate Memorial Day data inserted Succefully
    Confederate Heroes' Day data inserted Succefully
    State Holiday data inserted Succefully
    Confederate Memorial Day data inserted Succefully
    World Day for Safety and Health at Work data inserted Succefully
    Day of Remembrance for all Victims of Chemical Warfare data inserted Succefully
    Yom Ha'atzmaut data inserted Succefully
    International Jazz Day data inserted Succefully
    Loyalty Day data inserted Succefully
    Kentucky Oaks data inserted Succefully
    Lei Day data inserted Succefully
    Law Day data inserted Succefully
    World Tuna Day data inserted Succefully
    National Explosive Ordnance Disposal (EOD) Day data inserted Succefully
    Kentucky Derby data inserted Succefully
    World Press Freedom Day data inserted Succefully
    Kent State Shootings Remembrance data inserted Succefully
    Rhode Island Independence Day data inserted Succefully
    Cinco de Mayo data inserted Succefully
    National Nurses Day data inserted Succefully
    National Day of Prayer data inserted Succefully
    Time of Remembrance and Reconciliation for Those Who Lost Their Lives during the Second World War data inserted Succefully
    Truman Day data inserted Succefully
    Victory in Europe Day data inserted Succefully
    World Ovarian Cancer Day data inserted Succefully
    Military Spouse Appreciation Day data inserted Succefully
    World Migratory Bird Day data inserted Succefully
    Confederate Memorial Day data inserted Succefully
    Confederate Memorial Day data inserted Succefully
    Mother's Day data inserted Succefully
    Confederate Memorial Day data inserted Succefully
    Primary Election Day data inserted Succefully
    Lag BaOmer data inserted Succefully
    International Nurses Day data inserted Succefully
    Peace Officers Memorial Day data inserted Succefully
    International Day of Families data inserted Succefully
    National Defense Transportation Day data inserted Succefully
    Armed Forces Day data inserted Succefully
    Preakness Stakes data inserted Succefully
    World Information Society Day data inserted Succefully
    Lailat al-Qadr data inserted Succefully
    Emergency Medical Services for Children Day data inserted Succefully
    World Autoimmune / Autoinflammatory Arthritis Day data inserted Succefully
    Ascension Day data inserted Succefully
    World Day for Cultural Diversity for Dialogue and Development data inserted Succefully
    Harvey Milk Day data inserted Succefully
    International Day for Biological Diversity data inserted Succefully
    National Maritime Day data inserted Succefully
    International Day to End Obstetric Fistula data inserted Succefully
    Eid al-Fitr data inserted Succefully
    African Liberation Day data inserted Succefully
    Memorial Day data inserted Succefully
    National Missing Children's Day data inserted Succefully
    Jefferson Davis' Birthday data inserted Succefully
    Shavuot data inserted Succefully
    International Day of United Nations Peacekeepers data inserted Succefully
    World No Tobacco Day data inserted Succefully
    Pentecost data inserted Succefully
    Whit Monday data inserted Succefully
    Jefferson Davis' Birthday data inserted Succefully
    Statehood Day data inserted Succefully
    Global Day of Parents data inserted Succefully
    Jefferson Davis' Birthday data inserted Succefully
    International Day of Innocent Children Victims of Aggression data inserted Succefully
    World Environment Day data inserted Succefully
    Belmont Stakes data inserted Succefully
    D-Day data inserted Succefully
    Trinity Sunday data inserted Succefully
    World Oceans Day data inserted Succefully
    Corpus Christi data inserted Succefully
    Kamehameha Day data inserted Succefully
    World Day Against Child Labour data inserted Succefully
    International Albinism Awareness Day data inserted Succefully
    World Blood Donor Day data inserted Succefully
    Bunker Hill Day data inserted Succefully
    Army Birthday data inserted Succefully
    Flag Day data inserted Succefully
    World Elder Abuse Awareness Day data inserted Succefully
    International Day of Family Remittances data inserted Succefully
    World Day to Combat Desertification and Drought data inserted Succefully
    Juneteenth data inserted Succefully
    International Day for the Elimination of Sexual Violence in Conflict data inserted Succefully
    Emancipation Day data inserted Succefully
    World Refugee Day data inserted Succefully
    West Virginia Day data inserted Succefully
    June Solstice data inserted Succefully
    American Eagle Day data inserted Succefully
    Father's Day data inserted Succefully
    International Day of Yoga data inserted Succefully
    International Widows' Day data inserted Succefully
    Public Service Day data inserted Succefully
    Day of the Seafarer data inserted Succefully
    International Day in Support of Victims of Torture data inserted Succefully
    International Day Against Drug Abuse and Illicit Trafficking data inserted Succefully
    International Asteroid Day data inserted Succefully
    Independence Day data inserted Succefully
    Independence Day data inserted Succefully
    International Day of Cooperatives data inserted Succefully
    World Population Day data inserted Succefully
    Nathan Bedford Forrest Day data inserted Succefully
    Bastille Day data inserted Succefully
    World Youth Skills Day data inserted Succefully
    Nelson Mandela Day data inserted Succefully
    Pioneer Day data inserted Succefully
    Parents' Day data inserted Succefully
    National Korean War Veterans Armistice Day data inserted Succefully
    World Hepatitis Day data inserted Succefully
    International Day of Friendship data inserted Succefully
    World Day against Trafficking in Persons data inserted Succefully
    Tisha B'Av data inserted Succefully
    Eid al-Adha data inserted Succefully
    Colorado Day data inserted Succefully
    Raksha Bandhan data inserted Succefully
    Coast Guard Birthday data inserted Succefully
    Purple Heart Day data inserted Succefully
    International Day of the World's Indigenous People data inserted Succefully
    Victory Day data inserted Succefully
    Janmashtami data inserted Succefully
    International Youth Day data inserted Succefully
    Assumption of Mary data inserted Succefully
    Bennington Battle Day data inserted Succefully
    Bennington Battle Day data inserted Succefully
    National Aviation Day data inserted Succefully
    World Humanitarian Day data inserted Succefully
    Muharram data inserted Succefully
    Hawaii Statehood Day data inserted Succefully
    Senior Citizens Day data inserted Succefully
    Ganesh Chaturthi data inserted Succefully
    International Day for the Remembrance of the Slave Trade and its Abolition data inserted Succefully
    Women's Equality Day data inserted Succefully
    Lyndon Baines Johnson Day data inserted Succefully
    International Day against Nuclear Tests data inserted Succefully
    International Day of the Victims of Enforced Disappearances data inserted Succefully
    International Overdose Awareness Day data inserted Succefully
    World Sexual Health Day data inserted Succefully
    International Day of Charity data inserted Succefully
    Labor Day data inserted Succefully
    International Literacy Day data inserted Succefully
    California Admission Day data inserted Succefully
    World Suicide Prevention Day data inserted Succefully
    Patriot Day data inserted Succefully
    Carl Garner Federal Lands Cleanup Day data inserted Succefully
    International Programmers' Day data inserted Succefully
    International Day for South-South Cooperation data inserted Succefully
    National Grandparents Day data inserted Succefully
    International Day of Democracy data inserted Succefully
    International Day for the Preservation of the Ozone Layer data inserted Succefully
    Constitution Day and Citizenship Day data inserted Succefully
    Air Force Birthday data inserted Succefully
    National POW/MIA Recognition Day data inserted Succefully
    Rosh Hashana data inserted Succefully
    National CleanUp Day data inserted Succefully
    Rosh Hashana data inserted Succefully
    International Day of Peace data inserted Succefully
    September Equinox data inserted Succefully
    Emancipation Day data inserted Succefully
    International Celebrate Bisexuality Day data inserted Succefully
    World Maritime Day data inserted Succefully
    Native American Day data inserted Succefully
    International Day for the Total Elimination of Nuclear Weapons data inserted Succefully
    Gold Star Mother's Day data inserted Succefully
    World Tourism Day data inserted Succefully
    Yom Kippur data inserted Succefully
    Yom Kippur data inserted Succefully
    World Rabies Day data inserted Succefully
    World Heart Day data inserted Succefully
    International Day of Older Persons data inserted Succefully
    World Vegetarian Day data inserted Succefully
    International Day of Non-Violence data inserted Succefully
    First Day of Sukkot data inserted Succefully
    Feast of St Francis of Assisi data inserted Succefully
    World Habitat Day data inserted Succefully
    Child Health Day data inserted Succefully
    World Teachers' Day data inserted Succefully
    World Cerebral Palsy Day data inserted Succefully
    World Sight Day data inserted Succefully
    World Post Day data inserted Succefully
    Leif Erikson Day data inserted Succefully
    Last Day of Sukkot data inserted Succefully
    World Mental Health Day data inserted Succefully
    Shmini Atzeret data inserted Succefully
    International Day of the Girl Child data inserted Succefully
    Simchat Torah data inserted Succefully
    Indigenous People's Day data inserted Succefully
    Columbus Day data inserted Succefully
    Native Americans' Day data inserted Succefully
    Columbus Day data inserted Succefully
    Navy Birthday data inserted Succefully
    International Day for Natural Disaster Reduction data inserted Succefully
    White Cane Safety Day data inserted Succefully
    International Day of Rural Women data inserted Succefully
    Boss's Day data inserted Succefully
    World Food Day data inserted Succefully
    Navratri data inserted Succefully
    Sweetest Day data inserted Succefully
    International Day for the Eradication of Poverty data inserted Succefully
    Alaska Day data inserted Succefully
    Alaska Day data inserted Succefully
    World Statistics Day data inserted Succefully
    World Development Information Day data inserted Succefully
    United Nations Day data inserted Succefully
    Dussehra data inserted Succefully
    World Day for Audiovisual Heritage data inserted Succefully
    World Stroke Day data inserted Succefully
    The Prophet's Birthday data inserted Succefully
    Nevada Day data inserted Succefully
    Halloween data inserted Succefully
    World Cities Day data inserted Succefully
    Daylight Saving Time ends data inserted Succefully
    New York City Marathon data inserted Succefully
    All Saints' Day data inserted Succefully
    World Vegan Day data inserted Succefully
    International Day to End Impunity for Crimes against Journalists data inserted Succefully
    All Souls' Day data inserted Succefully
    Election Day data inserted Succefully
    Election Day data inserted Succefully
    Return Day data inserted Succefully
    International Day for Preventing the Exploitation of the Environment in War and Armed Conflict data inserted Succefully
    World Science Day for Peace and Development data inserted Succefully
    Marine Corps Birthday data inserted Succefully
    Veterans Day data inserted Succefully
    World Pneumonia Day data inserted Succefully
    Friday the 13th data inserted Succefully
    World Diabetes Day data inserted Succefully
    Diwali/Deepavali data inserted Succefully
    World Day of Remembrance for Road Traffic Victims data inserted Succefully
    International Day for Tolerance data inserted Succefully
    World Prematurity Day data inserted Succefully
    World Toilet Day data inserted Succefully
    World Philosophy Day data inserted Succefully
    International Men's Day data inserted Succefully
    Universal Children's Day data inserted Succefully
    Africa Industrialization Day data inserted Succefully
    World Television Day data inserted Succefully
    International Day for the Elimination of Violence against Women data inserted Succefully
    Thanksgiving Day data inserted Succefully
    Presidents' Day data inserted Succefully
    Acadian Day data inserted Succefully
    Day After Thanksgiving data inserted Succefully
    Day After Thanksgiving data inserted Succefully
    State Holiday data inserted Succefully
    Lincoln's Birthday/Lincoln's Day data inserted Succefully
    Black Friday data inserted Succefully
    Family Day data inserted Succefully
    American Indian Heritage Day data inserted Succefully
    First Sunday of Advent data inserted Succefully
    International Day of Solidarity with the Palestinian People data inserted Succefully
    Cyber Monday data inserted Succefully
    World AIDS Day data inserted Succefully
    Rosa Parks Day data inserted Succefully
    Rosa Parks Day data inserted Succefully
    International Day for the Abolition of Slavery data inserted Succefully
    International Day of Persons with Disabilities data inserted Succefully
    International Volunteer Day for Economic and Social Development data inserted Succefully
    World Soil Day data inserted Succefully
    St Nicholas Day data inserted Succefully
    Pearl Harbor Remembrance Day data inserted Succefully
    International Civil Aviation Day data inserted Succefully
    Feast of the Immaculate Conception data inserted Succefully
    World Genocide Commemoration Day data inserted Succefully
    International Anti-Corruption Day data inserted Succefully
    Human Rights Day data inserted Succefully
    Chanukah/Hanukkah (first day) data inserted Succefully
    International Mountain Day data inserted Succefully
    Feast of Our Lady of Guadalupe data inserted Succefully
    National Guard Birthday data inserted Succefully
    Bill of Rights Day data inserted Succefully
    Wright Brothers Day data inserted Succefully
    Pan American Aviation Day data inserted Succefully
    Last Day of Chanukah data inserted Succefully
    International Migrants Day data inserted Succefully
    Arabic Language Day data inserted Succefully
    International Human Solidarity Day data inserted Succefully
    December Solstice data inserted Succefully
    Christmas Eve data inserted Succefully
    Christmas Eve data inserted Succefully
    Christmas Day data inserted Succefully
    Day After Christmas Day data inserted Succefully
    Kwanzaa (first day) data inserted Succefully
    Day After Christmas Day data inserted Succefully
    New Year's Eve data inserted Succefully
    New Year's Eve data inserted Succefully


## Produce informative error if file doesn't exist to save


```python
try:
    with open('holiday.sqlite') as file:
        print("File present")
except FileNotFoundError:
    print('File is not present')
```

    File present


## Convert from JSON to CSV (local file) written to disk


```python
data = pd.read_json('https://holidays.abstractapi.com/v1/?api_key=574b7d06a79c4794a2e390123a19b857&country=US&year=2020')
data.to_csv('holiday.csv', index=None)
```

## Modify number of columns from source to destination: CSV file

### Remove columns: date_day, date_month, date_year, description, language, location, name_local


```python
data.drop(['date', 'description', 'language', 'location', 'name_local'], inplace=True, axis=1)
```

### Add date column that combines weekday, date day, month, and year of holiday


```python
data["date"] = (data["week_day"].astype(str) + " " + data["date_month"].astype(str) + "/" + data["date_day"].astype(str) + "/" + data["date_year"].astype(str))
data.drop(['date_day', 'date_month', 'date_year', 'week_day'], inplace=True, axis=1)
```

### Reorder columns


```python
data = data[['name', 'date', 'type', 'country']]
```

### Update new CSV file written to disk (local file)


```python
data.to_csv('holiday.csv', index=None)
```

## Produce informative error if file doesn't exist to update


```python
try:
    with open('holiday.csv') as file:
        read_data = file.read()
except FileNotFoundError as fnf_error:
    print(fnf_error)
```

## Brief summary of data file ingestion for SQLite database

### Connecting to sqlite
#### Connection object


```python
connection_obj = sqlite3.connect('holiday.sqlite')
```

#### Cursor object


```python
cursor_obj = connection_obj.cursor()
```

### How many records and columns are in the SQLite database:


```python
print("Number of Records: ")
cursor_obj.execute("SELECT * FROM HOLIDAY")
print(len(cursor_obj.fetchall()))

print("Number of Columns: ")
cursor_obj.execute("SELECT * FROM pragma_table_info('HOLIDAY')")
print(len(cursor_obj.fetchall()))
```

    Number of Records: 
    848
    Number of Columns: 
    4


### Close the connection


```python
connection_obj.close()
```

## Brief summary of data file ingestion for CSV file

### How many records and columns are in the CSV file:


```python
total_records = len(data.axes[0])
total_columns = len(data.axes[1])
print("Number of Records: "+ str(total_records))
print("Number of Columns: "+ str(total_columns))
```

    Number of Records: 424
    Number of Columns: 4

