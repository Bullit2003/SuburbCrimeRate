SELECT NAME, CRIME_CATEGORY.OFFENCE, CRIME_CATEGORY.SUBCATEGORY, START_DATE, END_DATE, RATE FROM CRIME_RATE 
JOIN SUBURB ON SUBURB.ROWID = CRIME_RATE.SUBURB_ID
JOIN CRIME_CATEGORY ON CRIME_CATEGORY.ROWID = CRIME_RATE.CRIME_CATEGORY_ID
WHERE SUBURB.ROWID = 4134