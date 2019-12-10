SELECT NAME, CRIME_CATEGORY.OFFENCE, CRIME_CATEGORY.SUBCATEGORY, START_DATE, END_DATE, RATE 
FROM SUBURB 
JOIN CRIME_RATE ON SUBURB.SUBURB_ID = CRIME_RATE.SUBURB_ID
JOIN CRIME_CATEGORY ON CRIME_CATEGORY.CRIME_CATEGORY_ID = CRIME_RATE.CRIME_CATEGORY_ID
WHERE NAME = 'Wattle Grove'
ORDER BY START_DATE ASC