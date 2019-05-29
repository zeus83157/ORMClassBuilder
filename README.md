# ORMClassBuilder

>物件關聯對映(Object Relational Mapping, ORM)以類別表示資料庫的結
>構，除方便程式設計師操作資料庫外，也免除SQL injection的風險。
>SQLAlchemy是Python社群中被廣泛使用的開源ORM套件
>本專案以Python針對MSSQL自動建立SQLAlchemy所需之類別，方便用戶使用。

## 所需套件
* pyodbc
* os

## 產生類別
1. 修改app.py檔案內的連接字串
    * Server = "[目標的伺服器名稱]"
    * user = "[登入帳號]"
    * password = "[登入密碼]"
    * database = "[目標資料庫名稱]"
    
2. 執行後即可獲得名為DatabaseBuilder.py的類別檔案

## 使用此類別
### 在你的程式中引用DatabaseBuild
    from DatabaseBuilder import
### 建立資料庫實例
    database = Session()
###  使用SQLAlchemy的API操作資料庫
    //AllWords為資料表
    stpwrd = database.query(AllWords.Words).filter(AllWords.Status == False).all() 