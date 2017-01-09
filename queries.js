module.exports = {
  get : 'SELECT ?? FROM ?? WHERE ?? = ?',
  insert : 'INSERT INTO ?? SET ?',
  update : 'UPDATE ?? SET ?',
  _delete : 'DELETE FROM ??',
  showTables : 'SHOW TABLES;',
  showTableDetails : 'SHOW COLUMNS FROM ??; SHOW INDEX FROM ??'
};
