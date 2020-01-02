#include "StdHeader.h"
#include "BooleanScorer.h"

#include "Scorer.h"
#include "Similarity.h"

#include <typeinfo>
// #include <iostream>

using namespace NSLib::util;
namespace NSLib{ namespace search{


BooleanScorer::BooleanScorer():
  coordFactors (NULL),
  maxCoord (1),
  currentDoc ( 0 ),
  requiredMask (0),
  prohibitedMask (0),
  nextMask (1),
  scorers(NULL)
{  
  bucketTable = new BucketTable(*this);
}

BooleanScorer::~BooleanScorer(){
  delete bucketTable; //TODO:
  if ( coordFactors != NULL )
    delete coordFactors;
  if ( scorers != NULL )
    delete scorers;
}


void BooleanScorer::add(Scorer& scorer, const bool required, const bool prohibited) {
  // cerr << "[BoolS::add]";
  int mask = 0;
  if (required || prohibited) {
    if (nextMask == 0){
      // cerr << "[More than 32 required/prohibited clauses in query.]";
      _THROWC( "More than 32 required/prohibited clauses in query.");
    }
    mask = nextMask;
    nextMask = ( nextMask << 1 );
  } 
  //cerr << "[required:" <<required <<",prohibited:"<<prohibited;
  if (!prohibited)
    maxCoord++;

  if (prohibited)
    prohibitedMask |= mask;        // update prohibited mask
  else if (required)
    requiredMask |= mask;        // update required mask
  //cerr << ",mask:"<<mask<<"]";
  //scorer, HitCollector, and scorers is delete in the SubScorer
  scorers = new SubScorer(scorer, false, true,
        bucketTable->newCollector(mask), scorers);
}

void BooleanScorer::computeCoordFactors(){
  coordFactors = new float[maxCoord];
  // cerr <<" maxCoord:" << maxCoord <<",(";
  for (int i = 0; i < maxCoord; i++){
    coordFactors[i] = Similarity::coord(i, maxCoord);
    // cerr << "coordFactors["<<i<<"]:"<<coordFactors[i]<<",";
  }
    // coordFactors[maxCoord+1] = Similarity::coord(maxCoord, maxCoord);
  // cerr<<")";
}

void BooleanScorer::score(HitCollector& results, const int maxDoc) {
  if (coordFactors == NULL){
    // cerr<<"{[coordFactors=NULL]";
    computeCoordFactors();
  }
  // cerr << "{<bscf[0]:" << coordFactors[0] << ",bcf[1]" <<coordFactors[1];
  // cerr <<",bscf[4]:"<<coordFactors[4]<<">";
 //cerr << "[BS:score]";
  while (currentDoc < maxDoc) {
    currentDoc = min(currentDoc + BucketTable::SIZE, maxDoc);
   // cerr << "[currentDoc:"<<currentDoc<<"]";
    for (SubScorer* t = scorers; t != NULL; t = t->next)
      t->scorer.score(t->collector, currentDoc);
   // cerr << "[bucketTable->]";
    bucketTable->collectHits(results);
  }
 //cerr << "[BS:score]Ed";
}
  
SubScorer::SubScorer(Scorer& scr, const bool r, const bool p, HitCollector& c,
                     SubScorer* nxt):
    scorer ( scr ),
    required ( r ),
    prohibited ( p ),
    collector ( c ),
    next ( nxt )
{
}
SubScorer::~SubScorer(){
  delete &scorer;
  delete &collector;
  delete next;
}

Bucket::Bucket():
    doc(-1),
    score(0),
    bits(0),
    coord(0)
{
}
Bucket::~Bucket(){
}


BucketTable::BucketTable(BooleanScorer& scr):
  scorer(scr),
  first(NULL)
{
  // const float* coord = scr.coordFactors;
  // cerr << "{(cdf[0]:" << coord[0] << ",cdf[1]:" << coord[1] <<",cdf[4]:"<<coord[4]<<")";
}


void BucketTable::clear(){
  //delete first;
  first = NULL;
}

void BucketTable::collectHits(HitCollector& results) 
{
  const int required = scorer.requiredMask;
  const int prohibited = scorer.prohibitedMask;
  const float* coord = scorer.coordFactors;
  // cerr << "{(cMaxLen="<< scorer.maxCoord << ";";//c[0]:" << coord[0] << ",c[1]:" << coord[1] <<",c[4]:"<<coord[4]<<")";

  for (Bucket* bucket = first; bucket!=NULL; bucket = bucket->next) {
    if ((bucket->bits & prohibited) == 0 &&    // check prohibited
        (bucket->bits & required) == required)
    {// check required
      // cerr << "{[BucketT::cH bkdoc:" << bucket->doc<<",bkscore:"<< bucket->score ;
      // cerr<<"*coord["<< bucket->coord<<"]:" << coord[bucket->coord]<<"] ";
      results.collect(bucket->doc,      // add to results
                      bucket->score * coord[bucket->coord]);//IndexSearcher::collect
    }
  //cerr << "{(c[0]:" << coord[0] << ",c[1]:" << coord[1] <<",c[4]:"<<coord[4]<<")";
  }
  clear(); //first = NULL;          // reset for next round
}

int BucketTable::size() const { return SIZE; }

HitCollector& BucketTable::newCollector(const int mask) {
  //cerr << "\tBucketTable::newCollector " << this->first << endl;
  return *new Collector(mask, *this);
}


Collector::Collector(const int msk, BucketTable& bucketTbl):
  mask(msk),
  bucketTable(bucketTbl)
{
}
  
void Collector::collect(const int doc, const float score) {
  // cerr << "{[Cc ";
  BucketTable& table = bucketTable;
  const int i = doc & BucketTable::MASK;
  Bucket& bucket = table.buckets[i];
  // if (bucket == NULL){
  //   bucket = new Bucket();
  //   table.buckets[i] = bucket;
  // }
  // cerr << "table["<<i<<"],";
  // cerr << "<doc:" << doc<< ",bket.doc:" << bucket.doc << ">" << ",score:" << score << ",bket.score:" << bucket.score;
  // cerr <<",<bket.coord:" << bucket.coord;
  if (bucket.doc != doc) {        // invalid bucket
    bucket.doc = doc;        // set doc
    bucket.score = score;        // initialize score
    bucket.bits = mask;        // initialize mask
    bucket.coord = 1;        // initialize coord
    // cerr << "=";
    bucket.next = table.first;      // push onto valid list
    table.first = &bucket;
  } else {            // valid bucket
    bucket.score += score;        // increment score
    bucket.bits |= mask;        // add bits in mask
    bucket.coord++;          // increment coord
    // cerr << " TO ";
  }
  // cerr << bucket.coord <<">" << "]";
}

}}
