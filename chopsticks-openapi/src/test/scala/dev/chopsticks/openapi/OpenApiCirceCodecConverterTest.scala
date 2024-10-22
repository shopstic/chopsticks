package dev.chopsticks.openapi

import dev.chopsticks.openapi.OpenApiZioSchemas.ZioSchemaOps
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import zio.schema.{DeriveSchema, Schema}
import zio.schema.DeriveSchema.gen
import zio.Chunk

object OpenApiCirceCodecConverterTest {
  // OpenApiMap definition
  final case class OpenApiMap[K, V](key: K, value: V)

  object OpenApiMap {
    def fromMap[K, V](map: Map[K, V]): Chunk[OpenApiMap[K, V]] = map.map { case (k, v) => OpenApiMap(k, v) }.to(Chunk)
    def toMap[K, V](values: Iterable[OpenApiMap[K, V]]): Map[K, V] = values.map { e => e.key -> e.value }.toMap
    implicit def schema[K: Schema, V: Schema]: Schema[OpenApiMap[K, V]] = DeriveSchema.gen[OpenApiMap[K, V]]
  }

  // Person case class for customers
  final case class Person(name: String, age: Int)
  object Person extends OpenApiModel[Person] {
    implicit override lazy val zioSchema: Schema[Person] = DeriveSchema.gen[Person]
  }

  // Recursive ProductCategory case class
  final case class ProductCategory(name: String, subcategories: List[ProductCategory])
  object ProductCategory extends OpenApiModel[ProductCategory] {
    implicit lazy val zioSchema: Schema[ProductCategory] = DeriveSchema.gen[ProductCategory]
  }

  // Customers: Map of customer IDs to Person objects
  final case class Customers(value: Map[String, Person])
  object Customers extends OpenApiModel[Customers] {
    implicit lazy val zioSchema: Schema[Customers] = {
      Schema[Chunk[OpenApiMap[String, Person]]]
        .mapBoth(
          values => Customers(OpenApiMap.toMap(values)),
          m => OpenApiMap.fromMap(m.value)
        )
    }
  }

  // Customers: Map of product SKU to product name
  final case class ProductDetails(skuToProductName: Map[String, String])
  object ProductDetails extends OpenApiModel[ProductDetails] {
    implicit lazy val zioSchema: Schema[ProductDetails] = {
      Schema[Chunk[OpenApiMap[String, String]]]
        .mapBoth(
          values => ProductDetails(OpenApiMap.toMap(values)),
          m => OpenApiMap.fromMap(m.skuToProductName)
        )
    }
  }

  // Products: A map of SKUs to product names and the recursive product categories
  final case class Products(
    productDetails: ProductDetails,
    category: ProductCategory
  )
  object Products extends OpenApiModel[Products] {
    implicit lazy val zioSchema: Schema[Products] = DeriveSchema.gen
  }

  // ECommerceData: Main class holding both Products and Customers
  final case class ECommerceData(
    customers: Customers,
    products: Products
  )
  object ECommerceData extends OpenApiModel[ECommerceData] {
    implicit override lazy val zioSchema: Schema[ECommerceData] = DeriveSchema.gen
  }
}

final class OpenApiCirceCodecConverterTest extends AnyWordSpecLike with Assertions with Matchers {
  import io.circe.syntax._
  import io.circe.parser._
  import OpenApiCirceCodecConverterTest._

  "OpenApiCirceCodecConverterTest" should {
    "correctly encode and decode valid ECommerceData" in {
      val ecommerceData = ECommerceData(
        customers = Customers(Map("123" -> Person("John Doe", 30), "456" -> Person("Jane Doe", 25))),
        products = Products(
          productDetails = ProductDetails(Map("001" -> "Laptop", "002" -> "Smartphone")),
          category = ProductCategory(
            name = "Electronics",
            subcategories = List(
              ProductCategory("Laptops", List()),
              ProductCategory(
                "Phones",
                List(
                  ProductCategory("Smartphones", List()),
                  ProductCategory("Feature Phones", List())
                )
              )
            )
          )
        )
      )

      val json = ecommerceData.asJson.noSpaces
      val decoded = decode[ECommerceData](json)

      decoded shouldEqual Right(ecommerceData)
    }

    "fail decoding if a field is missing" in {
      val invalidJson =
        """
        {
          "customers": [
            {"key": "123", "value": {"name": "John Doe", "age": 30}},
            {"key": "456", "value": {"name": "Jane Doe", "age": 25}}
          ]
        }
        """

      val result = decode[ECommerceData](invalidJson)

      result.isLeft shouldEqual true
      result.left.toOption.get.getMessage should include("products")
    }

    "correctly handle nested product categories" in {
      val ecommerceData = ECommerceData(
        customers = Customers(Map("123" -> Person("John Doe", 30))),
        products = Products(
          productDetails = ProductDetails(
            skuToProductName = Map("001" -> "Laptop")
          ),
          category = ProductCategory(
            name = "Computers",
            subcategories = List(
              ProductCategory(
                "Laptops",
                List(
                  ProductCategory("Gaming Laptops", List()),
                  ProductCategory("Business Laptops", List())
                )
              ),
              ProductCategory("Desktops", List())
            )
          )
        )
      )

      val json = ecommerceData.asJson.noSpaces
      val decoded = decode[ECommerceData](json)

      decoded shouldEqual Right(ecommerceData)
    }

    "fail decoding if the SKU type is incorrect" in {
      val invalidJson =
        """
        {
          "customers": [
            {"key": "123", "value": {"name": "John Doe", "age": 30}}
          ],
          "products": {
            "productDetails": [
              {"key": "001", "value": 1234}
            ],
            "category": {
              "name": "Electronics",
              "subcategories": []
            }
          }
        }
        """

      val result = decode[ECommerceData](invalidJson)

      result.isLeft shouldEqual true
    }

    "correctly decode and handle multiple subcategories" in {
      val ecommerceData = ECommerceData(
        customers = Customers(Map("789" -> Person("Emily Smith", 28))),
        products = Products(
          productDetails = ProductDetails(
            skuToProductName = Map("003" -> "Tablet")
          ),
          category = ProductCategory(
            name = "Gadgets",
            subcategories = List(
              ProductCategory("Tablets", List()),
              ProductCategory("Smartwatches", List()),
              ProductCategory("Drones", List())
            )
          )
        )
      )

      val json = ecommerceData.asJson.noSpaces
      val decoded = decode[ECommerceData](json)

      decoded shouldEqual Right(ecommerceData)
    }

    "fail decoding if the category structure is invalid" in {
      val invalidJson =
        """
        {
          "customers": [
            {"key": "789", "value": {"name": "Emily Smith", "age": 28}}
          ],
          "products": {
            "productDetails": [
              {"key": "003", "value": "Tablet"}
            ],
            "category": {
              "name": "Gadgets",
              "subcategories": "invalid_subcategory_format"
            }
          }
        }
        """

      val result = decode[ECommerceData](invalidJson)

      result.isLeft shouldEqual true
    }
  }
}
