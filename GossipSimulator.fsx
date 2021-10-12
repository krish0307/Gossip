#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp

let clock = Diagnostics.Stopwatch()
let clock1 = Diagnostics.Stopwatch()

type ActionType =
    | SetUp of IActorRef []*string*string
    | Gossip of String
    | Start
    | RumorCount of String
    | PushSum
    | CalculatePS of Double * Double 
    | Terminate of String * Double * Double
    | SetMaxRumors of int

let nodeCounter = int (fsi.CommandLineArgs.[1])
let topology = string (fsi.CommandLineArgs.GetValue 2)
let protocol = string (fsi.CommandLineArgs.GetValue 3)
let mutable convergedNodeCount = 0
let diff= 10.0 ** -10.0
let dimension=int(floor((float nodeCounter) ** (1.0/3.0)))
let mutable existingNodeSet=nodeCounter
if topology="3d" || topology="imp3d" then
    existingNodeSet<-dimension*dimension*dimension
else
    existingNodeSet<-nodeCounter

let system = ActorSystem.Create("System")

let getRandArrElement =
  let rnd = Random()
  fun (arr : int[]) -> arr.[rnd.Next(arr.Length)]

let getAdjustedNeighbour =
    fun(id:int) -> 
        let mutable newNum:int=1
        if id>existingNodeSet then
            newNum<-id-existingNodeSet
        elif id<1 then
            newNum<-existingNodeSet+id
        else 
            newNum<-id
        newNum

                      
type Tracker(nodes: IActorRef [], topology: string, protocol: string) =
    inherit Actor()
    override x.OnReceive(msg) =
        match msg :?> ActionType with
        | Start -> 
                for i in 1..nodes.Length-1 do
                    nodes.[i] <! SetUp(nodes,topology,protocol)
                let randNode = System.Random().Next(1, existingNodeSet+1)
                printfn "Starting RadnodeVal %d" randNode         
                if protocol="gossip" then
                    clock.Start()      
                    nodes.[randNode]<! Gossip("I have a gossip")
                elif protocol="push-sum" then
                    clock.Start()
                    nodes.[randNode]<!PushSum
        | Terminate (proto,sum,weight)->
            convergedNodeCount <- convergedNodeCount+1
            if convergedNodeCount = (existingNodeSet) then
                clock.Stop()
                printfn "Time taken for convergence: %O" clock.Elapsed
                if proto="push-sum" then       
                    printfn "For Push-sum:- Sum is %f, Weight is %f, and SumEstimate %.10f" sum weight (sum/weight)
                Environment.Exit(0)
        | _ ->()


type Node(neighbours: int [], nodeNum: int) =
    inherit Actor()
    let neighboursForMe=neighbours
    let mutable network : IActorRef [] =[||]
    let mutable topo=""
    let mutable proto=""
    let mutable tracker:IActorRef = null
    let mutable rumorCount=0
    let mutable sum:Double=nodeNum |> double
    let mutable weight:Double= 1.0
    let mutable epochCounter=0
    override x.OnReceive(msg) =
        match msg :?> ActionType with
        | SetUp (nodes: IActorRef [],topology:string,protocol:string)->
            network<-nodes
            topo<-topology
            proto<-protocol
            tracker<- x.Sender
            
        | Gossip rumor -> 
                let randNode = getRandArrElement neighboursForMe
                rumorCount<-rumorCount+1
                if rumorCount>=10 then
                    printfn "Calling terminate %d" nodeNum
                    tracker<! Terminate("gossip",0.,0.)
                else
                    network.[randNode]<! Gossip(rumor)
        | PushSum  ->
            let randNode = getRandArrElement neighboursForMe
            sum<-sum/2.0
            weight<-weight/2.0
            network.[randNode]<! CalculatePS(sum, weight)
        | CalculatePS (s:Double,w:Double)->
            let newSum=sum+s
            let newWeight=weight+w
            let ratioDiff=sum/weight-newSum/newWeight |> abs
            if ratioDiff<diff then
                epochCounter<-epochCounter+1
            else 
                epochCounter<-0
            if epochCounter<3 then
                sum<-(sum+s)/2.0
                weight<-(weight+w)/2.0
                let index =getRandArrElement neighboursForMe
                network.[index]<! CalculatePS (sum,weight)
            else
                tracker<! Terminate ("push-sum",sum,weight)

        | _ -> ()

//--------program start

let nodeArrayOfActors = Array.zeroCreate (existingNodeSet+1)
let nodeList = [ 1 .. existingNodeSet]
let mutable numberArray: int [] = [||]
for i in 1 ..existingNodeSet do
   numberArray<-[|i|] |>Array.append numberArray


let rec getNeighbours (id: int) (topology: string) (nodeCount: int) : int [] =
    let mutable neighbourArray: int [] = [||]

    match topology with
        | "full" ->     
                    neighbourArray<-numberArray
            
        | "line" -> 
                if id=1 then
                    neighbourArray<- [|id+1|] |>Array.append neighbourArray
                elif id=nodeCount then
                    neighbourArray<- [|id-1|] |>Array.append neighbourArray
                else
                    neighbourArray<- [|id+1|] |>Array.append neighbourArray
                    neighbourArray<- [|id-1|] |>Array.append neighbourArray
        | "3d" ->
                let left=getAdjustedNeighbour (id-1)
                let right=getAdjustedNeighbour (id+1)
                let top=getAdjustedNeighbour (id+dimension)
                let down=getAdjustedNeighbour (id-dimension)
                let front=getAdjustedNeighbour (id+dimension*dimension)
                let back=getAdjustedNeighbour (id-dimension*dimension)
                neighbourArray<-[|left;right;top;down;front;back|]|> Array.append neighbourArray
        | "imp3d" ->
                neighbourArray<- getNeighbours id "3d" nodeCount |> Array.append neighbourArray
                let mutable randNode = System.Random().Next(1, nodeCount+1)
                
                while Array.contains randNode neighbourArray || randNode <> id do
                    randNode <- System.Random().Next(1, nodeCount+1)
                
                neighbourArray<-[|randNode|]|> Array.append neighbourArray
        | _ ->printfn "Wrong Topology is provided as input, Please pass either of these- full, line, 3d, imp3d"
    neighbourArray

clock1.Start
for i in nodeList do
    let neighbours = getNeighbours i topology existingNodeSet// Make sure neighbours is iterated from 0
    nodeArrayOfActors.[i] <- system.ActorOf(Props.Create(typeof<Node>, neighbours, i), "demo" + string (i))
    //Make sure nodeArrayOfActors is iterated from 1
// Parallel.ForEach(nodeList, fun i -> 
//                             let neighbours = getNeighbours i topology existingNodeSet// Make sure neighbours is iterated from 0
//                             nodeArrayOfActors.[i] <-system.ActorOf(Props.Create(typeof<Node>, neighbours, i), "demo" + string (i))
//                             ) |> ignore
clock1.Stop
printfn "Initialized actors in %O" clock1.Elapsed  

let trackerRef = system.ActorOf(Props.Create(typeof<Tracker>, nodeArrayOfActors,topology,protocol), "tracker")
        

trackerRef<! Start

System.Console.ReadLine() |> ignore