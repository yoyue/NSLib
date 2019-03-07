#include <boost/format.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include <pion/http/response_writer.hpp>

#include <chrono>
#include <thread>
#include <exception>
#include <fstream>
#include <sstream>
#include <string>
#include <future>

#include <Encrypt.h>
#include <NSLib.h>
#include "analysis/ChineseAnalyzer.h"

#include "Worker.h"

namespace {
  namespace Cfg {
    using std::string;

    static const string modelListKey(".models");
  }; // namespace Cfg
}; // namespace


namespace UBCService {

  void SearchThread(Worker* worker)
  {
    while (true) {
      if (worker->jobQueue.empty())
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      else
        worker->search();
    }
  }

  void InitSearchThread(Worker* worker, string database)
  {
    worker->searcherMap[database] = new IndexSearcher((worker->m_indexBase + database).c_str());
    /*worker->totalMap[database] = 0;
    worker->resultMap[database] = "";
    worker->groupMap[database] = "";*/
    cout << "Searcher " << database << " initialized." << endl;
  }

  Worker::Worker(std::string indexPath)
  {
    m_indexBase = "C:\\index\\";
    if (indexPath.size() > 0)
      m_indexBase = indexPath;
    m_index = "SearchDB";

    prv_readDatabaseNames();

    //ThreadVec pool;
    for (StrVecIter it = databaseNames.begin(); it != databaseNames.end(); it++) {
      string database = *it;
      std::thread* t = new thread(InitSearchThread, this, database);
      std::this_thread::sleep_for(std::chrono::milliseconds(500));

      t->join();
      delete t;

      //pool.push_back(t);
    }
    // for (int i = 0; i < (int)pool.size(); i++) {
    //  thread *t = pool[i];
    //  t->join();
    //  delete t;
    // }
    searchThread = new thread(SearchThread, this);
  }

  void Worker::prv_readDatabaseNames()
  {
    //ifstream infile(m_indexBase + "databases.lst");
    ifstream infile("databases.lst");

    string line;
    while (getline(infile, line)) {
      if (line.empty()) continue;
      cerr << "Find database: " << line << endl;
      databaseNames.push_back(line);
    }
  }

  void Worker::search(Headers header, request_ptr request_ptr,
    tcp::connection_ptr tcp_conn)
  {
    Job job = { header, request_ptr, tcp_conn };

    queueMutex.lock();
    jobQueue.push(job);
    queueMutex.unlock();
  }

  //string result = prv_search(database, wgroupby, showFields, numResults);
  void searcherThread(Worker* worker, string database, Query* query,
    u16string wgroupby, u16string showfield, int numResults)
  {
    WStringVec showFields;
    Headers::getShowFields(showFields, showfield);
    worker->searchInDatabase(database, query, wgroupby, showFields, numResults);
  }

  void Worker::searchInDatabase(string database, Query* query,
    u16string wgroupby, WStringVec& showFields, int numResults)
  {
    IndexSearcher* searcher = searcherMap[database];
    //string result = "{ \"stat\":[{\"database\":{\"" + database + "\":\"" ;
    //string result = " \"" + database + "\": { \"count\": \"" ;
    cerr << endl << "@@@@@@@@@ Worker::search: " << database << " numResults: " << numResults << endl;
    Hits* hits = NULL;
    try {
      if (query != NULL) {
        cerr << " $$$$$ WSearch  ... " << database << endl;
        hits = &searcher->search(*query, numResults, const_cast<char_t*>(wgroupby.c_str()));
        cerr << " $$$$$$$$$ WSearch done. " << database << endl;
      }
    }
    catch (std::exception e) {
      cerr << endl << "################ parsing error." << e.what() << endl;
      cerr << "exception caught: " << e.what() << endl;
    }
    catch (...) {
      cerr << "exception caught: Unknown error" << endl;
    }
    int count = 0;
    string result;
    string groupStr;
    if (hits != NULL) {
      count = NSL_HitCount(hits);
      if (count != 0) {
        const char* groupby_str = NSL_HitGroupby(hits);
        groupStr = boost::str(boost::format(", \"group\":{%s}")
              % groupby_str);
        //printf( "     keywords: %s\n", keywords );
        printf(" %s >>>> found %d results, groupby: %s\n",
          database.c_str(), count, groupby_str);

        result = ", \"result\":[";
         
        for (int i = 0; i < (std::min)(numResults, count); i++) {
          
          string record = (i == 0) ? "{" : ",{";
          void* doc = NSL_Hit(hits, i);
    
          for (int j = 0; j < (int)showFields.size(); j++) {
            if (j != 0) record += ",";
            const char16_t* it = showFields[j].c_str();
            char* showField = NSL_wideToChar(it);//it->c_str()
            //printf( "%s: %ld\n", showField.c_str(), doc);
            char16_t* val = NSL_WGet_Field(doc, it);//it->c_str()
            if (val != NULL) {
              std::u16string valStr = val;
              int len = valStr.length() * sizeof(char16_t);
              size_t base64_output_length;
              char* base64_output = Base64::encode((const unsigned char*)val, len,
                base64_output_length);
              record += boost::str(boost::format(" \"%s\":\"%s\"")
                % showField % base64_output);
              NSL_WFree_Field(val);
              delete[] base64_output;
            }
            else {
              record += boost::str(boost::format(" \"%s\":\"\"") % showField);
            }
            delete[] showField;
          }

          record += "}";
          result += record;
        }

        result += "]";
      }
      delete ((Hits*)hits);
    }
    totalMap[database] = count;
    resultMap[database] = result;
    groupMap[database] = groupStr;
  }


  /*
  {
    "stat"：["database":{"index110":"1500","index100":"36000"},
       "group":{"1928": "39", "1929": "56"}],
    "result":
    [ { "RecordId": "26",
        "CodePath": "F0001,CA13C,CA13D,CA13DB0005",
        "Path": "", "PathInnerXml": "史部集成·傳記譜系·明·郭勳·三家世典（國朝典故本）·正文",
        "Title": "", "TitleInnerXml": "",
        "ImagePath": "6", "ImageOrderId": "6", "Extender": "",
        "FullText": "天下已大定，而梁王把匝剌瓦爾密實元之遺孽，獨恃雲南險遠，執我信使，納我逋逃。"},
       {  "RecordId": "36",
        "CodePath": "F0001,CA13C,CA13D,CA13DB0005", "Path": ""
      }
    ]
  }
  */

  void Worker::search()
  {
    Job& job = jobQueue.front();
    Headers& header = job.header;

    high_resolution_clock::time_point _start = high_resolution_clock::now();

    StringVec databases;

    for (StrVecIter it = header.databaseVec.begin(); it != header.databaseVec.end(); it++) {
      string database = *it;
      if (searcherMap.find(database) == searcherMap.end())
        continue;
      databases.push_back(database);
      cout << "db: " << database << endl;
    }

    std::u16string wquery = header.searchQuery,
      wfield = header.field,
      wgroupby = header.groupby;
    int numResults = header.numResults;

    int qlen = wquery.length() * sizeof(char16_t);
    string result = qlen == 0 ? "{}" : "{";

    if (qlen != 0) {
      char* wq = (char*)wquery.c_str();
      for (int i = 0; i < qlen; i++)
        cerr << hex << (int)(unsigned char)wq[i] << " ";
      cerr << endl;
      wprintf(L" query: %hs \n", wquery.c_str());

      ChineseAnalyzer analyzer;
      QueryParser qp(wfield.c_str(), analyzer);
      Query* query = &qp.Parse(wquery.c_str());

      //string* results = new string[header.databaseVec.size()]; 
      ThreadVec pool;
      for (auto database : databases)
      {
        std::thread* t = new thread(searcherThread, this, database, query,
          wgroupby, header.showfield, numResults);
        pool.push_back(t);
      }
      /*for (StrVecIter it = databases.begin(); it != databases.end(); it++) {
        string database = *it;

        std::thread* t = new thread(searcherThread, this, database, query,
          wgroupby, header.showfield, numResults);
        pool.push_back(t);
      }*/
      std::this_thread::sleep_for(nanoseconds(1000));
      int showTotal = 0;
      bool isgroupby = !ws2str(const_cast<char_t*>(wgroupby.c_str())).empty();
      //bool isgroupby = !wgroupby.empty();
      for (int i = 0; i < (int)pool.size(); i++) {
        thread *t = pool[i];
        t->join();
        delete t;
        if (i != 0)
          result += ", ";

        string dbName = databases[i];
        int count = totalMap[dbName];

        result += boost::str(boost::format(" \"%s\": {\"count\":\"%d\"")
          % dbName % count);
        if (count != 0) {
          // if (!groupMap[dbName].empty())
          //   result += boost::str(boost::format(", \"group\":{%s}")
          //     % groupMap[dbName]);
          if (isgroupby)
            result += groupMap[dbName];

          if (showTotal < numResults) {
            result += resultMap[dbName];
            showTotal += count;
          }
        }
        result += "}";
      }
      result += "}";
      delete query;
    }
    cerr << getTimeString() << "writing results" << endl;

    // Set Content-type to "text/plain" (plain ascii text)
    response_writer_ptr writer(response_writer::create(job.tcp_conn, *job.http_request_ptr,
      boost::bind(&tcp::connection::finish, job.tcp_conn)));
    writer->get_response().set_content_type(http::types::CONTENT_TYPE_TEXT);
    writer << result << http::types::STRING_CRLF;
    writer->send();

    nanoseconds ns = duration_cast<nanoseconds>(high_resolution_clock::now() - _start);
    cout << getTimeString() << "Search succeeded in " << (ns.count() / 1000000) << " ms" << endl;

    queueMutex.lock();
    jobQueue.pop();
    queueMutex.unlock();
  }

  void Worker::prv_addEscapeChar(char* src, char* des)
  {
    char* ps = src, *ps_last = ps, *pd = des;
    bool gb18030Found = false;
    for (int i = 0; i < (int)strlen(src); i++, ps++) {
      if (gb18030Found) {
        gb18030Found = false;
        continue;
      }
      if (*(unsigned char*)ps >= 0x80) {
        gb18030Found = true;
        continue;
      }
      if (*ps == '"' || *ps == '\\') {
        strncpy(pd, ps_last, ps - ps_last);
        pd += ps - ps_last;
        *(pd++) = '\\';
        *(pd++) = *ps;
        ps_last = ps + 1;
      }
    }
    strcpy(pd, ps_last);
  }

} // namespace UBCService
