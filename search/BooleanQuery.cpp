#include "StdHeader.h"
#include "BooleanQuery.h"

#include "index/IndexReader.h"
#include "util/StringBuffer.h"
#include "SearchHeader.h"
#include "BooleanClause.h"
#include "BooleanScorer.h"
#include "Scorer.h"

// #include <iostream>
using namespace NSLib::index;
namespace NSLib{ namespace search {

    /** Constructs an empty boolean query. */
  BooleanQuery::BooleanQuery() {
    clauses.setDoDelete(NSLib::util::DELETE_TYPE_DELETE);
    // cerr << "[BQ:Init]";
  }
    
  BooleanQuery::~BooleanQuery(){
  }
    
  const char_t* BooleanQuery::getQueryName() const{
    return _T("BooleanQuery");
  }

  void BooleanQuery::add(Query& query, const bool deleteQuery,
                         const bool required, const bool prohibited)
  {
    clauses.push_back(new BooleanClause(query, deleteQuery, required, prohibited));
  }
    
  /** Adds a clause to a boolean query. */
  void BooleanQuery::add(BooleanClause& clause) {
    clauses.push_back(&clause);
  }
    
  void BooleanQuery::prepare(IndexReader& reader) {
    // cerr << "[BQ::prepare]";
    for (uint i = 0 ; i < clauses.size(); i++) {
      BooleanClause* c = clauses.at(i);
      c->query.prepare(reader);
    }
    // cerr << "[BQ::prepare]Ed ";
  }
      
  //added by search highlighter
  BooleanClause** BooleanQuery::getClauses()
  {
    BooleanClause** ret = new BooleanClause*[clauses.size()];
    for (uint i = 0; i < clauses.size(); i++)
      ret[i] = clauses.at(i);
    return ret;
  }
    
  float BooleanQuery::sumOfSquaredWeights(Searcher& searcher){
    float sum = 0.0f;
    // cerr << "[BQ:sumOf] [size:";
    // cerr << clauses.size() << "]";
    for (uint i = 0 ; i < clauses.size(); i++) {
      BooleanClause* c = clauses.at(i);
      // cerr << " at[" << i <<"]";
      if (!c->prohibited)
        sum += c->query.sumOfSquaredWeights(searcher); // sum sub-query weights   TQ
      // else
        // cerr << " aot[" << i << "]";
    }
    // cerr << "[BQ:sum="<<sum<<"]";
    // cerr << "[BQ:sumOf]Ed";
    return sum;
  }
    
  void BooleanQuery::normalize(const float norm) {
    // cerr << "[BQ:normalize] [size:";
    // cerr << clauses.size() << "]";
    for (uint i = 0 ; i < clauses.size(); i++) {
      BooleanClause* c = clauses.at(i);
      if (!c->prohibited)
        c->query.normalize(norm);
    }
    // cerr << "[BQ:normalize]Ed";
  }
    
  Scorer* BooleanQuery::scorer(IndexReader& reader){   
    // cerr << "[BQ::scorer] size:";
    // cerr << clauses.size();
    // if (clauses.size() == 1) {   // optimize 1-term queries
    //   BooleanClause* c = clauses.at(0);
    //   if (!c->prohibited)        // just return term scorer
    //   {
    //     Scorer*  sc =c->query.scorer(reader);

    //     cerr << " [BQ::scorer]Ed. ";
    //     return sc;
    //   }
    // }
    BooleanScorer* result = new BooleanScorer;
    for (uint i = 0 ; i < clauses.size(); i++) {
      BooleanClause* c = clauses.at(i);
      Scorer* subScorer = c->query.scorer(reader);
      if (subScorer != NULL)
        result->add(*subScorer, c->required, c->prohibited);
      else if (c->required){
        // cerr << "[rtn NULL]";
        return NULL;
      }
    }

    // cerr << " [BQ::scorer]Ed ";
    return result;
  }
    
  // Prints a user-readable version of this query.
  const char_t* BooleanQuery::toString(const char_t* field) {
    NSLib::util::StringBuffer buffer;
    if(clauses.size()==0) return _T(" ");

    for (uint i = 0 ; i < clauses.size(); i++) {
      BooleanClause* c = clauses.at(i);
      if (c->prohibited)
        buffer.append(_T("-"));
      else if (c->required)
        buffer.append(_T("+"));

      Query& subQuery = c->query;
      if (subQuery.instanceOf(_T("BooleanQuery"))) { // wrap sub-bools in parens
        BooleanQuery& bq = (BooleanQuery&)subQuery;
        buffer.append(_T("("));

        const char_t* buf = c->query.toString(field);
        buffer.append(buf);
        delete[] buf;

        buffer.append(_T(")"));
      } else {
        const char_t* buf = c->query.toString(field);
        //cerr <<"("<< ws2str(buf) <<")";
        buffer.append(buf);
        delete[] buf;
      }
      if (i != clauses.size() - 1)
        buffer.append(_T(" "));
    }
    //cerr << " buffer[" << ws2str(buffer.ToString()) << "]";
    return buffer.ToString();
  }    

}}
