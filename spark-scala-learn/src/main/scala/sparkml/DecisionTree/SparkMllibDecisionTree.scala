package sparkml.DecisionTree



import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 决策树使用案例－出去玩
  * **********************************决策树********************************
  * 决策树是一种监督学习，监督学习，就是给定一对样本，每个样本都有一组属性和一个类别，
  * 这些类别是事先确定的，那么通过学习得到一个分类器，这个分类器能够对新出现的对象给
  * 出正确的分类．其原理是：从一组无序无规则的因素中归纳总结出符合要求的分类规则．
  *
  * 决策树算法基础：信息熵，ID3
  * 信息熵：对事件中不确定的信息的度量．一个事件或属性中，其信息熵越大，含有的不确定信
  *   息越大，对数据分析的计算也越有益．故，信息熵的选择总是选择当前事件中拥有最高
  *   信息熵的那个属性作为待测属性．
  * ID3：一种贪心算法，用来构造决策树．以信息熵的下降速度作为测试属性的标准，即在每个
  *   节点选取还尚未被用来划分的，具有最高信息增益的属性作为划分标准，然后继续这个过程，
  *   直到生成的决策树能完美分类训练样例．
  *
  * 使用场景：任何一个只要符合　key-value　模式的分类数据都可以根据决策树进行推断．
  *
  * 决策树用来预测的对象是固定的，丛根到叶子节点的一条特定路线就是一个分类规则，决定
  * 一个分类算法和结果．
  *
  * **********************************决策树********************************
  * https://my.oschina.net/sunmin/blog/720115
  */
object SparkMllibDecisionTree {
  val conf = new SparkConf()                                     //创建环境变量
    .setMaster("local")                                             //设置本地化处理
    .setAppName("ZombieBayes")                              //设定名称
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val data = MLUtils.loadLibSVMFile(sc, "./src/SparkMllibOtheralgorithm/DTree.txt")

    val numClasses = 2//分类数量
    val categorycalFeaturesInfo = Map[Int, Int]()//设定输入格式
    val impurity = "entropy" //设定信息增益计算方式
    val maxDepth = 5 //最大深度
    val maxBins = 3 //设定分割数据集

    val model = DecisionTree.trainClassifier(
      data,//输入数据集
      numClasses,//分类数量，本例只有出去，不出去，共两类
      categorycalFeaturesInfo,// 属性对格式，这里是单纯的键值对
      impurity,//计算信息增益形式
      maxDepth,// 树的高度
      maxBins//能够分裂的数据集合数量
    )

    println(model.topNode)
    println(model.numNodes)//5
    println(model.algo)//Classification
  }
}
