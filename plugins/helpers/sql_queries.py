#!/usr/bin/python
# -*- coding: utf-8 -*-


class SqlQueries:

    country_table_create = \
        """
                            CREATE TABLE IF NOT EXISTS country
                            (
                            code integer not null
                            constraint country_pkey primary key,
                            country_name varchar not null
                            )
                        """

    visa_table_create = \
        """
                         CREATE TABLE IF NOT EXISTS visa_type
                         (
                         visa_code integer,
                         visa_name varchar
                         )
                    """
    address_table_create = \
        """
                        CREATE TABLE IF NOT EXISTS address
                        (
                        code char(2),
                        state_name varchar
                        )
                    """

    port_table_create = \
        """
                        CREATE TABLE IF NOT EXISTS port
                        (
                        code char(3),
                        port_name varchar
                        )
                    """

    us_city_demography_table_create = \
        """
                            CREATE TABLE IF NOT EXISTS us_city_demography
                            (city VARCHAR,
                            state VARCHAR,
                            median_age NUMERIC,
                            male_population INT4,
                            female_population INT4,
                            totalpopulation INT4,
                            no_of_veterans INT4,
                            no_of_foreign_born INT4,
                            avg_house_size NUMERIC,
                            state_code VARCHAR,
                            race VARCHAR,
                            count_value INT4
                            )
                        """
    immigration_table_create = \
        """CREATE TABLE IF NOT EXISTS immigration 
                                (immigration_id int identity(1, 1) PRIMARY KEY,
                                 cicid int NOT NULL,
                                 i94yr int NOT NULL,
                                 i94mon int NOT NULL,
                                 i94cit int,
                                 i94res int,
                                 i94port char(3),
                                 arrdate int,
                                 i94mode int,
                                 i94addr char(3),
                                 depdate int,
                                 i94bir int,
                                 i94visa int,
                                 count int,
                                 dtadfile varchar,
                                 visapost char(3),
                                 occup char(3),
                                 entdepa char(1),
                                 entdepd char(1),
                                 entdepu char(1),
                                 matflag char(1),
                                 biryear int,
                                 dtaddto varchar,
                                 gender char(1),
                                 insnum varchar,
                                 airline char(3),
                                 admnum varchar,
                                 fltno varchar,
                                 visatype char(3)
                                );
                                """

    country_table_drop = """ DROP TABLE IF EXISTS country """

    visa_table_drop = """ DROP TABLE IF EXISTS visa_type """

    address_table_drop = """ DROP TABLE IF EXISTS address """

    port_table_drop = """ DROP TABLE IF EXISTS port """

    us_city_demography_table_drop = \
        """ DROP TABLE IF EXISTS
                                    us_city_demography """
    immigration_table_drop = \
        """ DROP TABLE IF EXISTS
                                    immigration """

    songplay_table_insert = \
        """
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """

    user_table_insert = \
        """
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """

    song_table_insert = \
        """
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """

    artist_table_insert = \
        """
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """

    time_table_insert = \
        """
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """

    analyze_data_select = \
        """
            select
                a.state_name,
                d.state_code,
                i.count_of_travellers,
                d.no_of_foreign_born     
            from
                (select
                    i94addr,
                    count(*) count_of_travellers                          
                from
                    immigration i                            
                group by
                    i94addr) i,
                (select
                    state_code,
                    nvl(sum(no_of_foreign_born),
                    0) no_of_foreign_born                          
                from
                    us_city_demography                          
                group by
                    state_code)d,
                address a   
            where
                i.i94addr=d.state_code       
                and a.code=d.state_code
           """
    country_data_select = \
        """
                            select code, country_name
                            from country
                            """
    address_data_select = \
        """
                            select code, state_name
                            from address
                            """
    port_data_select = \
        """
                        select code, port_name
                        from port
                        """

    create_table_queries = [
        country_table_create,
        visa_table_create,
        address_table_create,
        port_table_create,
        us_city_demography_table_create,
        immigration_table_create,
        ]
    drop_table_queries = [
        country_table_drop,
        visa_table_drop,
        address_table_drop,
        port_table_drop,
        us_city_demography_table_drop,
        immigration_table_drop,
        ]


##    create_table_queries = [us_city_demography_table_create]
##    drop_table_queries = [us_city_demography_table_drop]
