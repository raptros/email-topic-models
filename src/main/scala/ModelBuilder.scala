import scalanlp.io._
import scalanlp.stage._
import scalanlp.stage.text._
import scalanlp.text.tokenize._
import scalanlp.pipes.Pipes.global._

import edu.stanford.nlp.tmt.stage._
import edu.stanford.nlp.tmt.model.lda._
import edu.stanford.nlp.tmt.model.llda._
import scalanlp.serialization.TypedCompanion0;


object MBConsts {
  val numTopics = 30
}

case class HTMLBuster() extends Transformer {
  val htmlElem = """<.+>"""
  override def apply(doc:Iterable[String]) = {
    doc.filter(word => !(htmlElem matches word))
  }
}
object HTMLBuster extends TypedCompanion0[HTMLBuster] {
  prepare()
}

object ModelBuilder extends App {
  val src = args(0)
  val dst = args(1)
  println("building model from " + src + ", and storing it under " + dst) 

  val data = CSVFile(src) ~> IDColumn(1) ~> Drop(1)

  val tokenizer = {
    WhitespaceTokenizer() ~>
    //HTMLBuster() ~>
    WordsAndNumbersOnlyFilter() ~>         // ignore non-words and non-numbers
    //SimpleEnglishTokenizer() ~>            // tokenize on space and punctuation
    CaseFolder() ~>                        // lowercase everything
    MinimumLengthFilter(3)                 // take terms with >=3 characters
  }

  val text = {
    data ~>                              // read from the source file
    Columns(4, 5) ~> Join(" ") ~>                         // select column containing text
    TokenizeWith(tokenizer) ~>             // tokenize with tokenizer above
    TermCounter() ~>                       // collect counts (needed below)
    TermMinimumDocumentCountFilter(4) ~>   // filter terms in <4 docs
    TermDynamicStopListFilter(30) ~>       // filter out 30 most common terms
    DocumentMinimumLengthFilter(5)         // take only docs with >=5 terms
  }
  
  val dataset = LDADataset(text)

  val params = LDAModelParams(numTopics = MBConsts.numTopics, dataset = dataset)

  val modelPath = file(dst + "/lda-"+dataset.signature + "-" + params.signature)
  
  TrainCVB0LDA(params, dataset, output=modelPath, maxIterations=1000)
  //TrainGibbsLDA(params, dataset, output=modelPath, maxIterations=1500)
  

  /*
  println(text.description)
  println("Terms in the stop list:");
  for (term <- text.meta[TermStopList]) {
      println("  " + term);
  }*/
}
