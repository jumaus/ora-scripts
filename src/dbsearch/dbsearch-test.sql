/***
  A simple search in the HR schema to demonstrate the functionality.
***/
SELECT *
FROM TABLE(DBSEARCH.FIND(
  'ols.n'
));

/***
  Date columns are converted according to the sessions NLS_DATE_FORMAT.
  To use a different format, alter your session for that.
***/
ALTER SESSION SET NLS_DATE_FORMAT = 'yymmdd';

SELECT *
FROM TABLE(DBSEARCH.FIND(
  '^0604'
));
