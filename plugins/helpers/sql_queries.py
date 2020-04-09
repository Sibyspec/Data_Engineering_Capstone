class SqlQueries:
    """
    This class has all the sql statements needed to populate the Fact and Dimensions tables.
    """
    listings_table_insert = ("""
    insert into public.listings (LISTING_ID,
        LISTING_URL,
        HOST_ID,
        LISTING_PROPERTY_TYPE,
        LISTING_ROOM_TYPE,
        NO_OF_BEDROOMS,
        NO_OF_BATHROOMS,
        NO_OF_BEDS,
        LISTING_SIZE_SQFT,
        LISTING_DAILY_PRICE,
        LISTING_WEEKLY_PRICE,
        LISTING_MONTHLY_PRICE,
        LISTING_SECURITY_DEPOSIT,
        LISTING_CLEANING_FEES,
        MAXIMUM_GUESTS,
        MAXIMUM_NIGHTS,
        NO_OF_REVIEWS,
        REVIEW_SCORE_MAX_10,
        AVAILABLE_OUTOF_365)
        select
        LISTING_ID,
        LISTING_URL,
        HOST_ID,
        case
        when property_type='Aparthotel' then 1
        when property_type='Apartment' then 2
        when property_type='Barn' then 3
        when property_type='Bed and breakfast' then 4
        when property_type='Boat' then 5
        when property_type='Boutique hotel' then 6
        when property_type='Bungalow' then 7
        when property_type='Bus' then 8
        when property_type='Cabin' then 9
        when property_type='Camper/RV' then 10
        when property_type='Campsite' then 11
        when property_type='Casa particular (Cuba)' then 12
        when property_type='Castle' then 13
        when property_type='Chalet' then 14
        when property_type='Condominium' then 15
        when property_type='Cottage' then 16
        when property_type='Dome house' then 17
        when property_type='Earth house' then 18
        when property_type='Farm stay' then 19
        when property_type='Guest suite' then 20
        when property_type='Guesthouse' then 21
        when property_type='Hostel' then 22
        when property_type='Hotel' then 23
        when property_type='House' then 24
        when property_type='Houseboat' then 25
        when property_type='Island' then 26
        when property_type='Lighthouse' then 27
        when property_type='Loft' then 28
        when property_type='Other' then 29
        when property_type='Serviced apartment' then 30
        when property_type='Tiny house' then 31
        when property_type='Tipi' then 32
        when property_type='Townhouse' then 33
        when property_type='Villa' then 34
        when property_type='Yurt' then 35
        else 36
        end as listing_property_type,
        case when room_type='Private room' then 1
        when room_type='Entire home/apt' then 2
        when room_type='Hotel room' then 3
        when room_type='Shared room' then 4
        else 5 
        end as LISTING_ROOM_TYPE,
        NO_OF_BEDROOMS,
        NO_OF_BATHROOMS,
        NO_OF_BEDS,
        LISTING_SIZE_SQFT,
        LISTING_DAILY_PRICE,
        LISTING_WEEKLY_PRICE,
        LISTING_MONTHLY_PRICE,
        LISTING_SECURITY_DEPOSIT,
        LISTING_CLEANING_FEES,
        MAXIMUM_GUESTS,
        MAXIMUM_NIGHTS,
        NO_OF_REVIEWS,
        REVIEW_SCORE_MAX_10,
        AVAILABLE_OUTOF_365
        from public.staging_listings 
        where 
        listing_id is not null and 
        host_id is not null and 
        listing_property_type is not null and 
        listing_room_type is not null and 
        NO_OF_REVIEWS is not null
    """)

    host_table_insert = ("""insert into public.hosts
        (host_id,
        host_url,host_name,host_since,host_about,host_location,host_is_superhost,host_listings_count,host_identity_verified)
        SELECT host_id,
        host_url,
        host_name,
        to_date('01-01-2020','DD-MM-YYYY')-(host_since/100) as host_since,
        host_about,
        host_location,
        case when host_is_superhost='t' then True else False end as host_is_superhost,
        host_listings_count,
        case when host_identity_verified='t' then True else False end as host_identity_verified
        FROM staging_listings
        WHERE host_id is not null and 
        host_name is not null and 
        host_since is not null and 
        host_is_superhost is not null and 
        host_identity_verified is not  null
    """)

    review_table_insert = ("""insert into public.listing_reviews (listing_id, review_id, review_date, reviewer_id,reviewer_name, review_comments)
        SELECT listing_id, review_id, review_date, reviewer_id,reviewer_name, review_comments
        FROM staging_reviews where listing_id is not null and review_id is not null and reviewer_id is not null and review_comments is not null
    """)

    property_table_insert = ("""insert into public.listing_property_type (property_type_id, property_type)
        select distinct property_type_id, property_type from (select case
        when property_type='Aparthotel' then 1
        when property_type='Apartment' then 2
        when property_type='Barn' then 3
        when property_type='Bed and breakfast' then 4
        when property_type='Boat' then 5
        when property_type='Boutique hotel' then 6
        when property_type='Bungalow' then 7
        when property_type='Bus' then 8
        when property_type='Cabin' then 9
        when property_type='Camper/RV' then 10
        when property_type='Campsite' then 11
        when property_type='Casa particular (Cuba)' then 12
        when property_type='Castle' then 13
        when property_type='Chalet' then 14
        when property_type='Condominium' then 15
        when property_type='Cottage' then 16
        when property_type='Dome house' then 17
        when property_type='Earth house' then 18
        when property_type='Farm stay' then 19
        when property_type='Guest suite' then 20
        when property_type='Guesthouse' then 21
        when property_type='Hostel' then 22
        when property_type='Hotel' then 23
        when property_type='House' then 24
        when property_type='Houseboat' then 25
        when property_type='Island' then 26
        when property_type='Lighthouse' then 27
        when property_type='Loft' then 28
        when property_type='Other' then 29
        when property_type='Serviced apartment' then 30
        when property_type='Tiny house' then 31
        when property_type='Tipi' then 32
        when property_type='Townhouse' then 33
        when property_type='Villa' then 34
        when property_type='Yurt' then 35
        else
        36
        end as property_type_id,
        property_type
        from public.staging_listings)
    """)

    room_table_insert = ("""insert into public.listing_room_type (room_type_id,room_type)
        select distinct room_type_id,room_type from (SELECT case when room_type='Private room' then 1
        when room_type='Entire home/apt' then 2
        when room_type='Hotel room' then 3
        when room_type='Shared room' then 4
        else 5 end as room_type_id, 
        room_type 
        from public.staging_listings 
        where room_type is not null)
    """)

    address_table_insert = ("""insert into public.listing_address (listing_id,area,city,state,zipcode,country,latitude,longitude)
       select 
       listing_id,
       area,
       city,
       state,
       zipcode,
       country,
       latitude,
       longitude
       from public.staging_listings where listing_id is not null and zipcode is not null and country is not null
    """)