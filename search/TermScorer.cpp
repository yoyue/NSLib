#include "StdHeader.h"
#include "TermScorer.h"

#include "index/Terms.h"
#include "Scorer.h"
#include "search/Similarity.h"

// #include <iostream>

using namespace NSLib::index;
namespace NSLib{ namespace search {

//TermScorer takes TermDocs and delets it when TermScorer is cleaned up
TermScorer::TermScorer(TermDocs& td, l_byte_t* _norms, const float w):
  termDocs(td),
  norms(_norms),
  weight(w),
  pointer(0),
  pointerMax(0),
  doc(-1)
{
  for (int i = 0; i < NSLIB_SCORE_CACHE_SIZE; i++)
    scoreCache[i] = Similarity::tf(i) * weight;
  
  // for (int i = 0; i < 128; i++){
  //   docs[i] = 0;
  //   freqs[i] = 0;
  // }
  // cerr << "[w0=" << w << "]";
  pointerMax = termDocs.read(docs, freqs);    // fill buffers
     
  if (pointerMax != 0) // doc = 0;
    doc = docs[0];
  else {
    termDocs.close();     // close stream
    doc = INT_MAX;        // set to sentinel value
  }
}

TermScorer::~TermScorer(){
  delete &termDocs;
}

void TermScorer::score(HitCollector& c, const int end) {
  int d = doc;          // cache doc in local

  // cerr << endl << "{[TS:score]";
  // cerr << "[doc:" << d <<",end:" << end <<"]";

  while (d < end) {          // for docs in window
    // int f2 =  freqs[pointer];
    // if (f2 >= NSLIB_SCORE_CACHE_SIZE || f2 < 0){
    //   cerr <<"[f2]>=" << NSLIB_SCORE_CACHE_SIZE <<endl;
    //   //freqs[pointer] = 0;
    // }
   const int f = freqs[pointer];
   // cerr <<"f:"<<f;
   // cerr << ",scoreCache["<<f<<"]="<<scoreCache[f];
   // cerr << ",w=" << weight;
   // cerr << ",Stf=" << Similarity::tf(f) * weight;
    float score =          // compute tf(f)*weight
      f < NSLIB_SCORE_CACHE_SIZE && f >=0       // check cache
      ? scoreCache[f]        // cache hit
      : Similarity::tf(f) * weight;      // cache miss

    // cerr << ",score:" << score <<",sn[" << d <<"]:"<<Similarity::normf(norms[d]);
    score *= Similarity::normf(norms[d]);    // normalize for field
   
    // cerr << "[d:"<<d<<",score:" << score << "]"; 
    // cerr << "[p:"<<pointer<<",pMax:" << pointerMax << "]"; 
   c.collect(d, score);        // collect score
    if (++pointer >= pointerMax) {
      pointerMax = termDocs.read(docs, freqs);  // refill buffers
      if (pointerMax != 0) {
        pointer = 0;
      } else {
        termDocs.close();        // close stream
        doc = INT_MAX;      // set to sentinel value
         // doc=0;
        return;
      }
    }
    d = docs[pointer];
    // cerr << "[d2=" <<d<<"]";
  }
  doc = d;            // flush cache
    // cerr << "[d3=" <<d<<"]";
}
  
}}
