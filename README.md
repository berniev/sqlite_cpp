# cpp4sqlite: A C++ front-end

sqlite is a ubiquitous single-file database offering bindings for many languages.

sqlite does not have a pure C++ interface. It does however offer a C/C++ interface which we will build on.

### Usage:
#### Establish connection:
    Connection(location, option)
    // location typically std::string or ":memory:"
    // option typically OpenOption::CREATEWRITE or OpenOption::READWRITE
#### _Connection_ functions:
    // Single shot, can contain multiple queries separated by semicolon
    std::vector<std::vector<std::pair<std::string, std::string>>> quickQuery(queryString)

    // Parameterised query
    prepare(std::string)       -> Statement                                                     

    affectedRows()             -> int                                                                    
    lastInsertId()             -> int                                                                  
    errorStr()                 -> std::string
#### _Statement_ functions:
    // Parameterised query
    execute(param1 .. paramN)  -> Resultset 

    // read file and write file to blob
    execute(filesystem::path)  -> void
#### _Resultset_ functions:
    // save blob to file                                                                  
    toFile(path, replace)      -> int // bytes transferred                
                                                                                          
    // All rows, all columns as colName/stringValue pair(s)                               
    table()                    -> std::vector<std::vector<std::pair<std::string,std::string>>>                 
                                                                                          
    // all columns of current result row and advance to next row. If false, no row   
    row()                      -> std::optional<std::vector<std::pair<std::string,std::string>>>                 
    rowS()                     -> std::optional<std::vector<std::string>>                                        
    rowT<Type1, Type2, ..>()   -> std::optional<std::tuple<std::optional<T>...>>              
                                                                                          
    // result as colName/stringValue pair(s)                                              
    field();                   -> std::pair<std::string,std::string>                                
    field(int)                 -> std::pair<std::string,std::string>                                
    field(std::string)         -> std::pair<std::string,std::string>                                
    nextField()                -> std::pair<std::string,std::string>                                
                                                                                           
    // result as string                                                                    
    fieldS()                   -> std::string                                                      
    fieldS(int)                -> std::string                                                      
    fieldS(std::string)        -> std::string                                                      
    nextFieldS()               -> std::string                                                      
                                                                                          
    // result typed. false if db col is NULL. Maybe use value_or()                        
    fieldT<Type>()             -> std::optional<Type>                                         
    fieldT<Type>(int)          -> std::optional<Type>                                         
    fieldT<Type>(std::string)  -> std::optional<Type>                                         
    nextFieldT<Type>()         -> std::optional<Type>
#### Chaining (for single result only, else segfault likely):
    std::string result = connection->prepare(queryString)                                  
                                    .execute(params)                                       
                                    .fieldS()                                              
                                                                                           
    // rowT returns a tuple. Permits structured binding!                                   
    auto const [col1, col2, col3] = connection->prepare(queryString)                       
                                               .execute(params);                           
                                               .rowT<Type1, Type2, Type3>()                
                                               .value()                                    
