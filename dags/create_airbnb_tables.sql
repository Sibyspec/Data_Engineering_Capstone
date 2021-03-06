CREATE TABLE IF NOT EXISTS public.staging_listings (
	LISTING_ID int,
	LISTING_URL text,
	HOST_ID int,
	HOST_URL text,
	HOST_NAME text,
    HOST_SINCE int,
    HOST_LOCATION text,
    HOST_ABOUT varchar(65535),
    HOST_IS_SUPERHOST text,
    HOST_LISTINGS_COUNT int,
    HOST_IDENTITY_VERIFIED text,
    AREA text,
    CITY text,
	STATE text,
	ZIPCODE text not null,
	COUNTRY text,
	LATITUDE NUMERIC(18,0),
	LONGITUDE NUMERIC(18,0),
    PROPERTY_TYPE text,
    ROOM_TYPE text,
    MAXIMUM_GUESTS int,
    NO_OF_BATHROOMS float,
	NO_OF_BEDROOMS float,
	NO_OF_BEDS float,
	LISTING_SIZE_SQFT float,
	LISTING_DAILY_PRICE float,
	LISTING_WEEKLY_PRICE float,
	LISTING_MONTHLY_PRICE float,
	LISTING_SECURITY_DEPOSIT float,
	LISTING_CLEANING_FEES float,
	MAXIMUM_NIGHTS int,
    AVAILABLE_OUTOF_365 int,
	NO_OF_REVIEWS int,
	REVIEW_SCORE_MAX_10 int
);

CREATE TABLE IF NOT EXISTS public.staging_reviews (
	LISTING_ID int,
	REVIEW_ID int,
	REVIEW_DATE date,
	REVIEWER_ID int,
	REVIEWER_NAME text,
	REVIEW_COMMENTS varchar(65535)
);

CREATE TABLE IF NOT EXISTS public.listing_address (
    ADRRESS_ID int identity(1000,1),
	LISTING_ID int not null,
	AREA text,
	CITY text,
	STATE text,
	ZIPCODE text not null,
	COUNTRY text not null,
	LATITUDE NUMERIC(18,0),
	LONGITUDE NUMERIC(18,0),
	CONSTRAINT ADRRESS_ID_PKEY PRIMARY KEY (ADRRESS_ID)
);

CREATE TABLE IF NOT EXISTS public.listing_room_type (
	ROOM_TYPE_ID int not null,
	ROOM_TYPE text not null,
	CONSTRAINT ROOM_ID_PKEY PRIMARY KEY (ROOM_TYPE_ID)
);

CREATE TABLE IF NOT EXISTS public.listing_property_type (
	PROPERTY_TYPE_ID int not null,
	PROPERTY_TYPE text not null,
	CONSTRAINT PROPERTY_ID_PKEY PRIMARY KEY (PROPERTY_TYPE_ID)
);

CREATE TABLE IF NOT EXISTS public.listing_reviews (
	LISTING_ID int not null,
	REVIEW_ID int not null,
	REVIEW_DATE date,
	REVIEWER_ID int not null,
	REVIEWER_NAME text,
	REVIEW_COMMENTS varchar(65535) not null,
	CONSTRAINT REVIEW_ID_PKEY PRIMARY KEY (REVIEW_ID)
);

CREATE TABLE IF NOT EXISTS public.hosts (
	HOST_ID int NOT NULL,
	HOST_URL text,
	HOST_NAME text not null,
	HOST_SINCE date not null,
	HOST_ABOUT varchar(65535),
	HOST_LOCATION text,
	HOST_IS_SUPERHOST boolean not null,
	HOST_LISTINGS_COUNT int,
	HOST_IDENTITY_VERIFIED boolean not null,
	CONSTRAINT HOST_ID_PKEY PRIMARY KEY (HOST_ID)
);

CREATE TABLE IF NOT EXISTS public.listings (
	LISTING_ID int NOT NULL,
	LISTING_URL text,
	HOST_ID int NOT NULL,
	LISTING_PROPERTY_TYPE int not null,
	LISTING_ROOM_TYPE int not null,
	NO_OF_BEDROOMS float,
	NO_OF_BATHROOMS float,
	NO_OF_BEDS float,
	LISTING_SIZE_SQFT float,
	LISTING_DAILY_PRICE float,
	LISTING_WEEKLY_PRICE float,
	LISTING_MONTHLY_PRICE float,
	LISTING_SECURITY_DEPOSIT float,
	LISTING_CLEANING_FEES float,
	MAXIMUM_GUESTS int,
	MAXIMUM_NIGHTS int,
	NO_OF_REVIEWS int not null,
	REVIEW_SCORE_MAX_10 int,
	AVAILABLE_OUTOF_365 int,
	CONSTRAINT LISTING_ID_PKEY PRIMARY KEY (LISTING_ID),
	FOREIGN KEY (HOST_ID) REFERENCES HOSTS (HOST_ID),
	FOREIGN KEY (LISTING_PROPERTY_TYPE) REFERENCES LISTING_PROPERTY_TYPE (PROPERTY_TYPE_ID),
	FOREIGN KEY (LISTING_ROOM_TYPE) REFERENCES listing_room_type (ROOM_TYPE_ID)
);