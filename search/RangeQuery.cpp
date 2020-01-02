#include "StdHeader.h"
#include "RangeQuery.h"

#include "SearchHeader.h"
#include "Scorer.h"
#include "BooleanQuery.h"
#include "TermQuery.h"

#include "index/Term.h"
#include "index/Terms.h"
#include "index/IndexReader.h"
#include "util/StringBuffer.h"

// #include <iostream>
using namespace NSLib::index;
using namespace NSLib::util;
namespace NSLib{ namespace search{

RangeQuery::RangeQuery(Term* LowerTerm, Term* UpperTerm, const bool Inclusive):
  inclusive(Inclusive),
  query (NULL),
  reader(NULL)
{
  // cerr << "[RQ:Init]";
  if (LowerTerm == NULL && UpperTerm == NULL){
    // cerr << "[At least one term must be non-NULL]";
    _THROWC( "At least one term must be non-NULL");
  }

  if (LowerTerm != NULL)
    lowerTerm = LowerTerm->pointer();
  if (UpperTerm != NULL)
    upperTerm = UpperTerm->pointer();
  
  if (LowerTerm != NULL && UpperTerm != NULL 
      && stringCompare(lowerTerm->Field(), upperTerm->Field()) != 0 ){
    // cerr << "[Both terms must be for the same field]";
    _THROWC( "Both terms must be for the same field" );

  }
}

RangeQuery::~RangeQuery(){
  lowerTerm->finalize();
  upperTerm->finalize();
  // cerr << "[~RQ]";
}

const char_t* RangeQuery::getQueryName() const{
  return _T("RangeQuery");
}
    
void RangeQuery::prepare(IndexReader& reader)
{
  // cerr << "[RQ:prepare]" ;
  this->query = NULL;
  this->reader = &reader;
  // cerr << "[RQ:prepare]Ed " ;
}
    
float RangeQuery::sumOfSquaredWeights(Searcher& searcher)
{
  // cerr << "[RQ:sumOfS" ;
  // query = getQuery();
  // if(query==NULL)
  //   cerr<<" sofs-query==NULL";
  // cerr <<"]";
  return getQuery()->sumOfSquaredWeights(searcher);
}
    
void RangeQuery::normalize(const float norm)
{
  // cerr << "[RQ:normalize norm:" << norm <<"," ;
  // if(query==NULL)
  //   cerr<<" normalize-query==NULL";
  // cerr <<"]";
  getQuery()->normalize(norm);
}
    
Scorer* RangeQuery::scorer(IndexReader& reader)
{
  // cerr << "[RQ:scorer " ;
  // if(query==NULL)
  //   cerr<<" scorer-query==NULL";
  // cerr <<"]";
  return getQuery()->scorer(reader);
}

BooleanQuery* RangeQuery::getQuery()
{
  // cerr << "[RQ:getQuery] " ;
  if (query != NULL){
    // cerr << "[query!=NULL] " ;
    return query;
  } 

  // if we have a lowerTerm, start there. otherwise, start at beginning
  if (lowerTerm == NULL) {
    // cerr << "[lowerTerm=NULL]";
    lowerTerm = new Term(getField(), _T(""));
  }
   
  // cerr << "[reader] " ;
  // cerr << "[ltf:"<<ws2str(lowerTerm->Field());
  // cerr << ",lt:"<<ws2str(lowerTerm->Text())<<"]";

  TermEnum& _enum = reader->getTerms(lowerTerm); //SegmentTermEnum& TermInfosReader::getTerms

  // cerr << "[reader2] " ;
  BooleanQuery* q = new BooleanQuery();
  _TRY {
    const char_t* lowerText = NULL;

    bool checkLower = false;
            
    if (!inclusive) { // make adjustments to set to exclusive
      if (lowerTerm != NULL) {
        lowerText = lowerTerm->Text();
        checkLower = true;
      }
      if (upperTerm != NULL) {
        // set upperTerm to an actual term in the index
        TermEnum& uppEnum = reader->getTerms(upperTerm);
        upperTerm = uppEnum.getTerm();
      }
    }

   const char_t* testField = getField();
   //bool isAdd = false;
   int st=0;
    do {
      Term* term = _enum.getTerm();
      if (term == NULL || stringCompare(term->Field(), testField) != 0 )
        break;

      if (!checkLower || stringCompare(term->Text(), lowerText) > 0) {
        checkLower = false;
        if (upperTerm != NULL) {
          int compare = upperTerm->compareTo( *term );
          // if beyond the upper term, or is exclusive and
          // this is equal to the upper term, break out
          if ((compare < 0) || (!inclusive && compare == 0)) 
            break;
        }
        //isAdd = true;
        TermQuery& tq = *new TermQuery(*term);	  // found a match
        tq.setBoost(boost);               // set the boost
        q->add(tq, true, false, false);		// add to q
        // st++;
      }
    } while (_enum.next());
    // cerr <<" [st=" << st <<"]";
  }
  _FINALLY (_enum.close(););
  // _FINALLY (_enum.close();delete &_enum;);
  query = q;
  // cerr << "[RQ:getQuery]Ed " ;
  return query;
}

/** Prints a user-readable version of this query. */
const char_t* RangeQuery::toString(const char_t* field)
{
  StringBuffer buffer;
  if (stringCompare(getField(), field) != 0 )
  {
    buffer.append(getField() );
    buffer.append( _T(":"));
  }
  buffer.append(inclusive ? _T("[") : _T("{"));
  buffer.append(lowerTerm != NULL ? lowerTerm->Text() : _T("NULL"));
  buffer.append(_T("-"));
  buffer.append(upperTerm != NULL ? upperTerm->Text() : _T("NULL"));
  buffer.append(inclusive ? _T("]") : _T("}"));
  if (boost != 1.0f)
  {
    buffer.append( _T("^"));
    buffer.append( boost, 4 );
  }
  return buffer.ToString();
}   
    
const char_t* RangeQuery::getField()
{
  return (lowerTerm != NULL ? lowerTerm->Field() : upperTerm->Field());
}

}}
