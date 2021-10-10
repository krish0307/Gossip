#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp

let clock = Diagnostics.Stopwatch()

type ActionType =
    | SetUp of IActorRef []*string*string
    // | IntializeAll of IActorRef []
    | Gossip of String
    | Start
    | RumorCount of String
    | PushSum
    | CalculatePS of Double * Double 
    | Terminate of String * Double * Double
    | SetMaxRumors of int
    | FailNode of bool
    | NodeFailed of string

let nodeCounter = int (fsi.CommandLineArgs.[1])
let failureNodes = int (fsi.CommandLineArgs.[2])
let topology = string (fsi.CommandLineArgs.GetValue 3)
let protocol = string (fsi.CommandLineArgs.GetValue 4)
let mutable convergedNodeCount = 0
let diff= 10.0 ** -10.0
let dimension=int(floor((float nodeCounter) ** (1.0/3.0)))
let mutable existingNodeSet=nodeCounter
if topology="3d" || topology="imp3d" then
    existingNodeSet<-dimension*dimension*dimension
else
    existingNodeSet<-nodeCounter

printfn "nodes %d" dimension

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

// let num=getAdjustedNeighbour 0                      
// printfn "%d" num 
                      
type Tracker(nodes: IActorRef [], topology: string, protocol: string) =
    inherit Actor()

    override x.OnReceive(msg) =
        match msg :?> ActionType with
        | Start -> 
                for i in 1..nodes.Length-1 do
                    nodes.[i] <! SetUp(nodes,topology,protocol)
                let randNode = System.Random().Next(1, existingNodeSet+1)

                printfn "Start RadnodeVal %d" randNode
                if protocol="gossip" then
                    clock.Start()
                    nodes.[randNode]<! Gossip("I have a gossip")
                elif protocol="push-sum" then
                    clock.Start()
                    printfn "RandNode at start %d " randNode 
                    nodes.[randNode]<!PushSum
        | Terminate (proto,sum,weight)->
            convergedNodeCount <- convergedNodeCount+1
            if convergedNodeCount = (existingNodeSet-failureNodes) then
                clock.Stop()
                printfn "Time taken for convergence: %O" clock.Elapsed
                if proto="push-sum" then       
                    printfn "For Push-sum:- Sum is %f, Weight is %f, and SumEstimate %.10f" sum weight (sum/weight)
                Environment.Exit(0)
            //TODO:kill logic
        | NodeFailed msg -> 
            printfn "%s %s" msg x.Sender.Path.Name
            // printfn "Converged %i nodes" numberOfConvergedNodes
        | _ ->()


type Node(creator: IActorRef, neighbours: int [], nodeId: int) =
    inherit Actor()
    let neighboursForMe=neighbours
    let mutable network : IActorRef [] =[||]
    let mutable topo=""
    let mutable proto=""
    let mutable tracker:IActorRef = null
    let mutable rumorCount=0
    let mutable sum:Double=nodeId |> double
    let mutable weight:Double= 1.0
    let mutable epochCounter=0
    let mutable shouldFail = false
    override x.OnReceive(msg) =
        match msg :?> ActionType with
        | FailNode boolVal -> 
            shouldFail <- boolVal
            printfn " fail node %d" nodeId
        | SetUp (nodes: IActorRef [],topology:string,protocol:string)->
            // printfn "INside"
            network<-nodes
            topo<-topology
            proto<-protocol
            tracker<- x.Sender
            
        | Gossip rumor -> 
                let randNode = getRandArrElement neighboursForMe
                // printfn "val i %d and rumorCount %d and randNode %d network size %d" nodeNum rumorCount randNode network.Length
                if rumorCount = 2 && shouldFail then
                    tracker <! NodeFailed("Failed")
                else if rumorCount>=10 then
                    printfn "Calling terminate %d" nodeId
                    tracker<! Terminate("gossip",0.,0.)
                else
                    rumorCount<-rumorCount+1
                    network.[randNode]<! Gossip(rumor)
        | PushSum  ->
            // let randNode = System.Random().Next(0, neighboursForMe.Length)
          
            let randNode = getRandArrElement neighboursForMe

            if rumorCount = 2 && shouldFail then
                tracker <! NodeFailed("Failed")
            else
                sum<-sum/2.0
                weight<-weight/2.0
                rumorCount<-rumorCount+1
                network.[randNode]<! CalculatePS(sum, weight)
        | CalculatePS (s:Double,w:Double)->
            let newSum=sum+s
            let newWeight=weight+w
            let ratioDiff=sum/weight-newSum/newWeight |> abs
            // printfn "val i %f " newWeight 
            // printfn "val i %f " ratioDiff 

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
let mutable numberArray: int [] = [||]//TODO:see if list to array conversion possible
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
                // printfn "%d" left
                let right=getAdjustedNeighbour (id+1)
                let top=getAdjustedNeighbour (id+dimension)
                let down=getAdjustedNeighbour (id-dimension)
                let front=getAdjustedNeighbour (id+dimension*dimension)
                let back=getAdjustedNeighbour (id-dimension*dimension)
                // printfn "id %d left %d right %d top %d down %d front %d back %d " id left right top down front back
                neighbourArray<-[|left;right;top;down;front;back|]|> Array.append neighbourArray
        | "imp3d" ->
                neighbourArray<- getNeighbours id "3d" nodeCount |> Array.append neighbourArray
                let mutable randNode = System.Random().Next(1, nodeCount+1)
                
                while Array.contains randNode neighbourArray || randNode <> id do
                    randNode <- System.Random().Next(1, nodeCount+1)
                
                neighbourArray<-[|randNode|]|> Array.append neighbourArray
        | _ ->printfn "Wrong Topology is provided as input, Please pass either of these- full, line, 3d, imp3d"
    neighbourArray


let trackerRef = system.ActorOf(Props.Create(typeof<Tracker>, nodeArrayOfActors,topology,protocol), "tracker")

for i in nodeList do
    let neighbours = getNeighbours i topology existingNodeSet// Make sure neighbours is iterated from 0
    nodeArrayOfActors.[i] <- system.ActorOf(Props.Create(typeof<Node>, trackerRef, neighbours, i), "demo" + string (i))
    //Make sure nodeArrayOfActors is iterated from 1

for f in [1..failureNodes] do
    let fnode = System.Random().Next(1, existingNodeSet+1)
    nodeArrayOfActors.[fnode] <! FailNode true

trackerRef<! Start

System.Console.ReadLine() |> ignore