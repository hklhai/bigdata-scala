package cn.edu.ncut

/**
  * Created by Ocean lin on 2017/10/18.
  */
class SecondSortKey(val first: Int, val second: Int) extends Ordered[SecondSortKey] with Serializable {


  override def compare(that: SecondSortKey) = {
    if (this.first - that.first != 0)
      this.first - that.first
    else
      this.second - that.second
  }


}
